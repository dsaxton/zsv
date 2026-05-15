use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_LINE_LEN: usize = 1024 * 1024;
const MAX_FIELDS: usize = 4096;
const READ_BUF_SIZE: usize = 256 * 1024;
const TABLE_SAMPLE_BUDGET: usize = 1024 * 1024;
const DEFAULT_HEAD_ROWS: usize = 10;
const MAX_RANK_ROWS: usize = 10_000;
const DEFAULT_DELIMITER: u8 = b',';

#[derive(Debug)]
pub enum CliError {
    Message(String),
    BrokenPipe,
    Io(io::Error),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Message(msg) => f.write_str(msg),
            CliError::BrokenPipe => f.write_str("broken pipe"),
            CliError::Io(err) => write!(f, "Error: {err}"),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::BrokenPipe {
            CliError::BrokenPipe
        } else {
            CliError::Io(err)
        }
    }
}

type CliResult<T> = Result<T, CliError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggFunc {
    Sum,
    Min,
    Max,
    Count,
    Mean,
}

impl AggFunc {
    fn name(self) -> &'static str {
        match self {
            AggFunc::Sum => "sum",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Count => "count",
            AggFunc::Mean => "mean",
        }
    }
}

#[derive(Clone, Debug)]
struct Agg {
    func: AggFunc,
    field: String,
    col_index: Option<usize>,
    total: f64,
    extreme: f64,
    n: usize,
    tainted: bool,
}

impl Agg {
    fn new(func: AggFunc, field: &str) -> Self {
        Self {
            func,
            field: field.to_string(),
            col_index: None,
            total: 0.0,
            extreme: 0.0,
            n: 0,
            tainted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Like,
}

#[derive(Clone, Debug)]
struct Filter {
    field: String,
    op: FilterOp,
    value: String,
    col_index: Option<usize>,
    value_num: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RankDirection {
    Greatest,
    Least,
}

#[derive(Clone, Debug)]
struct RankConfig {
    field: String,
    direction: RankDirection,
}

#[derive(Clone, Debug, Default)]
struct Config {
    selectors: Vec<String>,
    filters: Vec<Filter>,
    aggs: Vec<Agg>,
    inputs: Vec<String>,
    head: Option<usize>,
    tail: Option<usize>,
    rank: Option<RankConfig>,
    sample: Option<usize>,
    delimiter: u8,
    no_header: bool,
    input_no_header: bool,
    table: bool,
    validate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Field {
    value: Vec<u8>,
    quoted: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum ParseRecordError {
    TooManyFields,
    UnterminatedQuote,
    MalformedQuotedField,
}

impl ParseRecordError {
    fn message(&self) -> &'static str {
        match self {
            ParseRecordError::TooManyFields => "too many fields in row",
            ParseRecordError::UnterminatedQuote => "unterminated quoted field",
            ParseRecordError::MalformedQuotedField => "malformed quoted field",
        }
    }
}

#[derive(Clone, Debug)]
struct RankedRow {
    fields: Vec<Vec<u8>>,
    key: Vec<u8>,
    key_num: Option<f64>,
}

#[derive(Clone, Debug)]
struct SampleRow {
    fields: Vec<Vec<u8>>,
}

#[derive(Default)]
struct InputSchema {
    header: Option<Vec<Vec<u8>>>,
    cols: usize,
}

struct PendingLine {
    line: Vec<u8>,
}

struct SourceState {
    pending_first_data: Option<PendingLine>,
    line_no: usize,
}

struct TailBuffer {
    rows: Vec<Vec<u8>>,
    capacity: usize,
    start: usize,
    count: usize,
}

impl TailBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            rows: vec![Vec::new(); capacity],
            capacity,
            start: 0,
            count: 0,
        }
    }

    fn append(&mut self, row: Vec<u8>) {
        if self.count < self.capacity {
            let idx = (self.start + self.count) % self.capacity;
            self.rows[idx] = row;
            self.count += 1;
        } else {
            self.rows[self.start] = row;
            self.start = (self.start + 1) % self.capacity;
        }
    }

    fn flush<W: Write>(&self, writer: &mut W) -> CliResult<()> {
        for i in 0..self.count {
            let idx = (self.start + i) % self.capacity;
            writer.write_all(&self.rows[idx])?;
        }
        Ok(())
    }
}

struct Prng(u64);

impl Prng {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self(nanos ^ ((std::process::id() as u64) << 32) ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn range_less_than(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        (self.next_u64() % upper as u64) as usize
    }
}

pub fn run<R: Read, W: Write, E: Write>(
    args: &[String],
    stdin: &mut R,
    stdout: &mut W,
    stderr: &mut E,
) -> CliResult<()> {
    let config = match parse_args_list(args)? {
        ParsedArgs::Help => {
            stdout.write_all(usage().as_bytes())?;
            return Ok(());
        }
        ParsedArgs::Config(config) => config,
    };
    execute(config, stdin, stdout, stderr)
}

enum ParsedArgs {
    Help,
    Config(Config),
}

fn usage() -> &'static str {
    "Usage: zsv [OPTIONS]\n       zsv [OPTIONS] [FILE...]\n\nReads CSV from stdin and writes to stdout.\nIf FILEs are provided, they are processed in order and stacked as one CSV.\nUse - to read from stdin in a file list.\n\nOptions:\n  -s, --select FIELDS   Comma-separated column names or 1-based indices\n  -f, --filter EXPR     Filter expression: field op value\n                        Operators: =, !=, <, >, <=, >=, ~ (glob)\n                        Repeatable (multiple filters = AND)\n  -d, --delimiter DELIM Field delimiter (default comma; supports tab or \\t)\n  -n, --head [N]        Output first N data rows (after filtering; default 10 when omitted)\n      --tail [N]        Output last N data rows (after filtering; preserves header; default 10 when omitted)\n      --greatest FIELD  Output rows with the largest values in FIELD; use -n for count (default 10; max 10000)\n      --least FIELD     Output rows with the smallest values in FIELD; use -n for count (default 10; max 10000)\n      --sample N        Output uniform random sample of N rows (after filtering)\n      --agg FUNC:FIELD  Aggregate FIELD; FUNC: sum, min, max, count, mean\n                        Repeatable; incompatible with --greatest/--least and --head\n  -t, --table           Pretty-print output as an aligned table\n      --no-header       Suppress header row in output\n      --input-no-header Treat the first input row as data\n      --validate        Validate CSV structure (parse + column count)\n  -h, --help            Print this help message\n"
}

fn parse_delimiter(value: &str) -> Option<u8> {
    if value == "tab" || value == "\\t" {
        Some(b'\t')
    } else if value.len() == 1 {
        Some(value.as_bytes()[0])
    } else {
        None
    }
}

fn parse_optional_count(args: &[String], i: &mut usize, name: &str) -> CliResult<usize> {
    if *i + 1 >= args.len() {
        return Ok(DEFAULT_HEAD_ROWS);
    }
    let value = &args[*i + 1];
    match value.parse::<usize>() {
        Ok(n) => {
            *i += 1;
            Ok(n)
        }
        Err(_) if value.starts_with('-') => Ok(DEFAULT_HEAD_ROWS),
        Err(_) => Err(CliError::Message(format!(
            "Error: invalid {name} value: {value}"
        ))),
    }
}

fn parse_args_list(args: &[String]) -> CliResult<ParsedArgs> {
    let mut config = Config {
        delimiter: DEFAULT_DELIMITER,
        ..Config::default()
    };
    let mut rank_field: Option<String> = None;
    let mut rank_direction: Option<RankDirection> = None;
    let mut positional_mode = false;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if !positional_mode && arg == "--" {
            positional_mode = true;
        } else if !positional_mode && (arg == "-h" || arg == "--help") {
            return Ok(ParsedArgs::Help);
        } else if !positional_mode && (arg == "-t" || arg == "--table") {
            config.table = true;
        } else if !positional_mode && arg == "--no-header" {
            config.no_header = true;
        } else if !positional_mode && arg == "--input-no-header" {
            config.input_no_header = true;
        } else if !positional_mode && (arg == "-d" || arg == "--delimiter") {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --delimiter requires an argument".to_string())
            })?;
            config.delimiter = parse_delimiter(value).ok_or_else(|| {
                CliError::Message(format!("Error: invalid --delimiter value: {value}"))
            })?;
        } else if !positional_mode && (arg == "-s" || arg == "--select") {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --select requires an argument".to_string())
            })?;
            for field in value.split(',').filter(|s| !s.is_empty()) {
                config.selectors.push(field.to_string());
            }
        } else if !positional_mode && (arg == "-f" || arg == "--filter") {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --filter requires an argument".to_string())
            })?;
            let filter = parse_filter(value).ok_or_else(|| {
                CliError::Message(format!("Error: invalid filter expression: {value}"))
            })?;
            config.filters.push(filter);
        } else if !positional_mode && (arg == "-n" || arg == "--head") {
            config.head = Some(parse_optional_count(args, &mut i, "--head")?);
        } else if !positional_mode && arg == "--tail" {
            let n = parse_optional_count(args, &mut i, "--tail")?;
            if n == 0 {
                return Err(CliError::Message("Error: --tail must be >= 1".to_string()));
            }
            config.tail = Some(n);
        } else if !positional_mode && arg == "--agg" {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --agg requires an argument".to_string())
            })?;
            let agg = parse_agg(value).ok_or_else(|| {
                CliError::Message(format!("Error: invalid --agg expression: {value}"))
            })?;
            config.aggs.push(agg);
        } else if !positional_mode && arg == "--greatest" {
            if rank_field.is_some() {
                return Err(CliError::Message(
                    "Error: --greatest and --least are mutually exclusive".to_string(),
                ));
            }
            i += 1;
            rank_field = Some(
                args.get(i)
                    .ok_or_else(|| {
                        CliError::Message("Error: --greatest requires a field name".to_string())
                    })?
                    .clone(),
            );
            rank_direction = Some(RankDirection::Greatest);
        } else if !positional_mode && arg == "--least" {
            if rank_field.is_some() {
                return Err(CliError::Message(
                    "Error: --greatest and --least are mutually exclusive".to_string(),
                ));
            }
            i += 1;
            rank_field = Some(
                args.get(i)
                    .ok_or_else(|| {
                        CliError::Message("Error: --least requires a field name".to_string())
                    })?
                    .clone(),
            );
            rank_direction = Some(RankDirection::Least);
        } else if !positional_mode && arg == "--sample" {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --sample requires an argument".to_string())
            })?;
            let n = value.parse::<usize>().map_err(|_| {
                CliError::Message(format!("Error: invalid --sample value: {value}"))
            })?;
            if n == 0 {
                return Err(CliError::Message(
                    "Error: --sample must be >= 1".to_string(),
                ));
            }
            config.sample = Some(n);
        } else if !positional_mode && arg == "--validate" {
            config.validate = true;
        } else if arg == "-" || positional_mode || !arg.starts_with('-') || arg.is_empty() {
            positional_mode = true;
            config.inputs.push(arg.clone());
        } else {
            return Err(CliError::Message(format!("Error: unknown argument: {arg}")));
        }
        i += 1;
    }

    if let Some(field) = rank_field {
        let limit = config.head.unwrap_or(DEFAULT_HEAD_ROWS);
        if limit > MAX_RANK_ROWS {
            return Err(CliError::Message(format!(
                "Error: ranked output -n {limit} exceeds maximum of {MAX_RANK_ROWS}"
            )));
        }
        config.rank = Some(RankConfig {
            field,
            direction: rank_direction.expect("rank direction set with field"),
        });
    }

    if !config.aggs.is_empty() {
        if config.rank.is_some() {
            return Err(CliError::Message(
                "Error: --agg cannot be combined with --greatest/--least".to_string(),
            ));
        }
        if config.head.is_some() {
            return Err(CliError::Message(
                "Error: --agg cannot be combined with --head".to_string(),
            ));
        }
    }

    if config.sample.is_some() {
        if config.rank.is_some() {
            return Err(CliError::Message(
                "Error: --sample cannot be combined with --greatest/--least".to_string(),
            ));
        }
        if !config.aggs.is_empty() {
            return Err(CliError::Message(
                "Error: --sample cannot be combined with --agg".to_string(),
            ));
        }
        if config.head.is_some() {
            return Err(CliError::Message(
                "Error: --sample cannot be combined with --head".to_string(),
            ));
        }
    }

    if config.tail.is_some() {
        if config.head.is_some() {
            return Err(CliError::Message(
                "Error: --tail cannot be combined with --head".to_string(),
            ));
        }
        if config.rank.is_some() {
            return Err(CliError::Message(
                "Error: --tail cannot be combined with --greatest/--least".to_string(),
            ));
        }
        if !config.aggs.is_empty() {
            return Err(CliError::Message(
                "Error: --tail cannot be combined with --agg".to_string(),
            ));
        }
        if config.sample.is_some() {
            return Err(CliError::Message(
                "Error: --tail cannot be combined with --sample".to_string(),
            ));
        }
        if config.table {
            return Err(CliError::Message(
                "Error: --tail cannot be combined with --table".to_string(),
            ));
        }
    }

    if config.validate
        && (!config.selectors.is_empty()
            || !config.filters.is_empty()
            || !config.aggs.is_empty()
            || config.head.is_some()
            || config.rank.is_some()
            || config.sample.is_some()
            || config.table
            || config.tail.is_some())
    {
        return Err(CliError::Message(
            "Error: --validate cannot be combined with other output options".to_string(),
        ));
    }

    Ok(ParsedArgs::Config(config))
}

fn parse_filter(expr: &str) -> Option<Filter> {
    let ops = [
        ("!=", FilterOp::Neq),
        ("<=", FilterOp::Lte),
        (">=", FilterOp::Gte),
        ("=", FilterOp::Eq),
        ("~", FilterOp::Like),
        ("<", FilterOp::Lt),
        (">", FilterOp::Gt),
    ];
    for (text, op) in ops {
        if let Some(pos) = expr.find(text) {
            if pos == 0 {
                continue;
            }
            let field = expr[..pos].trim();
            let value = expr[pos + text.len()..].trim();
            return Some(Filter {
                field: field.to_string(),
                op,
                value: value.to_string(),
                col_index: None,
                value_num: value.parse::<f64>().ok(),
            });
        }
    }
    None
}

fn parse_agg(expr: &str) -> Option<Agg> {
    let (func_str, field) = expr.split_once(':')?;
    if func_str.is_empty() || field.is_empty() {
        return None;
    }
    let func = match func_str {
        "sum" => AggFunc::Sum,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "count" => AggFunc::Count,
        "mean" => AggFunc::Mean,
        _ => return None,
    };
    Some(Agg::new(func, field))
}

fn parse_record(line: &[u8], delimiter: u8) -> Result<Vec<Field>, ParseRecordError> {
    let mut fields = Vec::new();
    let mut i = 0;
    while i <= line.len() {
        if i == line.len() {
            if line.last() == Some(&delimiter) {
                push_field(&mut fields, Vec::new(), false)?;
            }
            break;
        }
        if line[i] == b'"' {
            i += 1;
            let mut value = Vec::new();
            let mut closed = false;
            while i < line.len() {
                if line[i] == b'"' {
                    if i + 1 < line.len() && line[i + 1] == b'"' {
                        value.push(b'"');
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else {
                    value.push(line[i]);
                    i += 1;
                }
            }
            if !closed {
                return Err(ParseRecordError::UnterminatedQuote);
            }
            push_field(&mut fields, value, true)?;
            if i == line.len() {
                break;
            } else if line[i] == delimiter {
                i += 1;
            } else {
                return Err(ParseRecordError::MalformedQuotedField);
            }
        } else {
            let start = i;
            while i < line.len() && line[i] != delimiter {
                i += 1;
            }
            push_field(&mut fields, line[start..i].to_vec(), false)?;
            if i < line.len() {
                i += 1;
            } else {
                break;
            }
        }
    }
    Ok(fields)
}

fn push_field(
    fields: &mut Vec<Field>,
    value: Vec<u8>,
    quoted: bool,
) -> Result<(), ParseRecordError> {
    if fields.len() >= MAX_FIELDS {
        return Err(ParseRecordError::TooManyFields);
    }
    fields.push(Field { value, quoted });
    Ok(())
}

fn read_next_line<R: BufRead + ?Sized>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<Option<Vec<u8>>> {
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', buf)?;
        if n == 0 {
            return Ok(None);
        }
        if buf.len() > MAX_LINE_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
        }
        if buf.ends_with(b"\n") {
            buf.pop();
        }
        if buf.ends_with(b"\r") {
            buf.pop();
        }
        if buf.is_empty() {
            continue;
        }
        return Ok(Some(buf.clone()));
    }
}

fn read_data_line<R: BufRead + ?Sized>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    state: &mut SourceState,
) -> io::Result<Option<Vec<u8>>> {
    if let Some(pending) = state.pending_first_data.take() {
        state.line_no += 1;
        return Ok(Some(pending.line));
    }
    let line = read_next_line(reader, buf)?;
    if line.is_some() {
        state.line_no += 1;
    }
    Ok(line)
}

fn resolve_column_index(header: &[Vec<u8>], selector: &str) -> CliResult<usize> {
    if let Ok(idx) = selector.parse::<usize>() {
        if (1..=header.len()).contains(&idx) {
            return Ok(idx - 1);
        }
        return Err(CliError::Message(format!(
            "Error: column index {idx} out of range (1-{})",
            header.len()
        )));
    }
    for (i, col) in header.iter().enumerate() {
        if col == selector.as_bytes() {
            return Ok(i);
        }
    }
    Err(CliError::Message(format!(
        "Error: unknown column: {selector}"
    )))
}

fn resolve_output_state(
    header: &[Vec<u8>],
    selectors: &[String],
    filters: &mut [Filter],
) -> CliResult<Option<Vec<usize>>> {
    let col_indices = if selectors.is_empty() {
        None
    } else {
        let mut indices = Vec::with_capacity(selectors.len());
        for selector in selectors {
            indices.push(resolve_column_index(header, selector)?);
        }
        Some(indices)
    };
    for filter in filters {
        filter.col_index = Some(resolve_column_index(header, &filter.field)?);
    }
    Ok(col_indices)
}

fn headers_match(expected: &[Vec<u8>], actual: &[Field]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(a, b)| a.as_slice() == b.value.as_slice())
}

fn init_input_source<R: BufRead + ?Sized>(
    reader: &mut R,
    source_name: &str,
    config: &Config,
    schema: &mut InputSchema,
    buf: &mut Vec<u8>,
) -> CliResult<Option<SourceState>> {
    let first_line =
        match read_next_line(reader, buf).map_err(|err| read_error(source_name, 1, err))? {
            Some(line) => line,
            None => return Ok(None),
        };
    let result = parse_record(&first_line, config.delimiter).map_err(|err| {
        CliError::Message(format!(
            "Error parsing CSV in {source_name} on line 1: {}",
            err.message()
        ))
    })?;
    if config.input_no_header {
        if schema.header.is_none() {
            schema.cols = result.len();
            schema.header = Some(make_synthetic_header(result.len()));
        } else if result.len() != schema.cols {
            return Err(CliError::Message(format!(
                "Error: column count mismatch in {source_name}: expected {}, got {}",
                schema.cols,
                result.len()
            )));
        }
        return Ok(Some(SourceState {
            pending_first_data: Some(PendingLine { line: first_line }),
            line_no: 0,
        }));
    }

    if let Some(header) = &schema.header {
        if !headers_match(header, &result) {
            return Err(CliError::Message(format!(
                "Error: header mismatch in {source_name}"
            )));
        }
    } else {
        schema.cols = result.len();
        schema.header = Some(result.into_iter().map(|f| f.value).collect());
    }
    Ok(Some(SourceState {
        pending_first_data: None,
        line_no: 1,
    }))
}

fn make_synthetic_header(count: usize) -> Vec<Vec<u8>> {
    (1..=count).map(|i| i.to_string().into_bytes()).collect()
}

fn read_error(source_name: &str, line_no: usize, err: io::Error) -> CliError {
    if err.kind() == io::ErrorKind::InvalidData && err.to_string() == "line too long" {
        CliError::Message(format!(
            "Error reading line {line_no} in {source_name}: line too long"
        ))
    } else {
        CliError::Io(err)
    }
}

fn parse_row_for_source(
    line: &[u8],
    config: &Config,
    source_name: &str,
    line_no: usize,
) -> CliResult<Vec<Field>> {
    parse_record(line, config.delimiter).map_err(|err| {
        CliError::Message(format!(
            "Error parsing CSV in {source_name} on line {line_no}: {}",
            err.message()
        ))
    })
}

fn execute<R: Read, W: Write, E: Write>(
    mut config: Config,
    stdin: &mut R,
    stdout: &mut W,
    stderr: &mut E,
) -> CliResult<()> {
    let input_paths = if config.inputs.is_empty() {
        vec!["-".to_string()]
    } else {
        config.inputs.clone()
    };
    let emit_input_header = !config.input_no_header && !config.no_header;
    let mut writer = io::BufWriter::new(stdout);
    let mut schema = InputSchema::default();

    if config.validate {
        let rows = validate_inputs(&input_paths, &config, stdin, &mut schema)?;
        writeln!(writer, "Valid: {rows} row(s)")?;
        writer.flush()?;
        return Ok(());
    }

    if !config.table
        && config.selectors.is_empty()
        && config.filters.is_empty()
        && config.rank.is_none()
        && config.aggs.is_empty()
        && config.sample.is_none()
    {
        fast_pass_through(
            &input_paths,
            &config,
            stdin,
            &mut writer,
            &mut schema,
            emit_input_header,
        )?;
        writer.flush()?;
        return Ok(());
    }

    if let Some(sample_n) = config.sample {
        sample_mode(
            &input_paths,
            &mut config,
            stdin,
            &mut writer,
            sample_n,
            emit_input_header,
        )?;
        writer.flush()?;
        return Ok(());
    }

    if config.rank.is_some() {
        rank_mode(
            &input_paths,
            &mut config,
            stdin,
            &mut writer,
            emit_input_header,
        )?;
        writer.flush()?;
        return Ok(());
    }

    if !config.aggs.is_empty() {
        agg_mode(&input_paths, &mut config, stdin, &mut writer, stderr)?;
        writer.flush()?;
        return Ok(());
    }

    if config.table {
        table_mode(
            &input_paths,
            &mut config,
            stdin,
            &mut writer,
            emit_input_header,
        )?;
        writer.flush()?;
        return Ok(());
    }

    csv_transform_mode(
        &input_paths,
        &mut config,
        stdin,
        &mut writer,
        emit_input_header,
    )?;
    writer.flush()?;
    Ok(())
}

fn with_source<R: Read, T>(
    input_path: &str,
    stdin: &mut R,
    f: impl FnOnce(&mut dyn BufRead, &str) -> CliResult<T>,
) -> CliResult<T> {
    if input_path == "-" {
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, stdin);
        f(&mut reader, "stdin")
    } else {
        let file = File::open(input_path).map_err(|err| {
            CliError::Message(format!("Error: failed to open {input_path}: {err}"))
        })?;
        let mut reader = BufReader::with_capacity(READ_BUF_SIZE, file);
        f(&mut reader, input_path)
    }
}

fn validate_inputs<R: Read>(
    input_paths: &[String],
    config: &Config,
    stdin: &mut R,
    schema: &mut InputSchema,
) -> CliResult<usize> {
    let mut rows_seen = 0;
    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, schema, &mut line_buf)?
            else {
                return Ok(());
            };
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if row.len() != schema.cols {
                    return Err(CliError::Message(format!(
                        "Error: column count mismatch in {source_name} on line {}: expected {}, got {}",
                        state.line_no,
                        schema.cols,
                        row.len()
                    )));
                }
                rows_seen += 1;
            }
            Ok(())
        })?;
    }
    Ok(rows_seen)
}

fn fast_pass_through<R: Read, W: Write>(
    input_paths: &[String],
    config: &Config,
    stdin: &mut R,
    writer: &mut W,
    schema: &mut InputSchema,
    emit_input_header: bool,
) -> CliResult<()> {
    let mut rows_written = 0;
    let mut header_emitted = false;
    let mut tail = config.tail.map(TailBuffer::new);

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            if config.input_no_header {
                while let Some(line) = read_next_line(reader, &mut line_buf)
                    .map_err(|err| read_error(source_name, rows_written + 1, err))?
                {
                    if config.head.is_some_and(|limit| rows_written >= limit) {
                        break;
                    }
                    write_raw_line_tail_aware(writer, tail.as_mut(), &line)?;
                    rows_written += 1;
                }
                return Ok(());
            }

            let Some(mut state) =
                init_input_source(reader, source_name, config, schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !header_emitted && emit_input_header {
                write_record(
                    writer,
                    schema.header.as_ref().unwrap(),
                    None,
                    config.delimiter,
                )?;
                header_emitted = true;
            }
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                if config.head.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                write_raw_line_tail_aware(writer, tail.as_mut(), &line)?;
                rows_written += 1;
            }
            Ok(())
        })?;
    }
    if let Some(tail) = tail {
        tail.flush(writer)?;
    }
    Ok(())
}

fn write_raw_line_tail_aware<W: Write>(
    writer: &mut W,
    tail: Option<&mut TailBuffer>,
    line: &[u8],
) -> CliResult<()> {
    if let Some(tail) = tail {
        let mut row = Vec::with_capacity(line.len() + 1);
        row.extend_from_slice(line);
        row.push(b'\n');
        tail.append(row);
    } else {
        writer.write_all(line)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn csv_transform_mode<R: Read, W: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    emit_input_header: bool,
) -> CliResult<()> {
    let mut schema = InputSchema::default();
    let mut col_indices: Option<Vec<usize>> = None;
    let mut output_ready = false;
    let mut header_emitted = false;
    let mut rows_written = 0;
    let mut tail = config.tail.map(TailBuffer::new);

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, &mut schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !output_ready {
                col_indices = resolve_output_state(
                    schema.header.as_ref().unwrap(),
                    &config.selectors,
                    &mut config.filters,
                )?;
                output_ready = true;
            }
            if !header_emitted && emit_input_header {
                write_record(
                    writer,
                    schema.header.as_ref().unwrap(),
                    col_indices.as_deref(),
                    config.delimiter,
                )?;
                header_emitted = true;
            }
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                if config.head.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if passes_filters(&config.filters, &row) {
                    write_fields_tail_aware(
                        writer,
                        tail.as_mut(),
                        &row,
                        col_indices.as_deref(),
                        config.delimiter,
                    )?;
                    rows_written += 1;
                }
            }
            Ok(())
        })?;
    }
    if let Some(tail) = tail {
        tail.flush(writer)?;
    }
    Ok(())
}

fn write_fields_tail_aware<W: Write>(
    writer: &mut W,
    tail: Option<&mut TailBuffer>,
    row: &[Field],
    col_indices: Option<&[usize]>,
    delimiter: u8,
) -> CliResult<()> {
    if let Some(tail) = tail {
        let mut buf = Vec::new();
        write_fields(&mut buf, row, col_indices, delimiter)?;
        tail.append(buf);
    } else {
        write_fields(writer, row, col_indices, delimiter)?;
    }
    Ok(())
}

fn sample_mode<R: Read, W: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    sample_n: usize,
    emit_input_header: bool,
) -> CliResult<()> {
    let mut schema = InputSchema::default();
    let mut col_indices: Option<Vec<usize>> = None;
    let mut ready = false;
    let mut rows_seen = 0;
    let mut reservoir: Vec<SampleRow> = Vec::new();
    let mut prng = Prng::new();

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, &mut schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !ready {
                col_indices = resolve_output_state(
                    schema.header.as_ref().unwrap(),
                    &config.selectors,
                    &mut config.filters,
                )?;
                ready = true;
            }
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if !passes_filters(&config.filters, &row) {
                    continue;
                }
                let cloned = clone_field_values(&row);
                if reservoir.len() < sample_n {
                    reservoir.push(SampleRow { fields: cloned });
                } else {
                    let j = prng.range_less_than(rows_seen + 1);
                    if j < sample_n {
                        reservoir[j] = SampleRow { fields: cloned };
                    }
                }
                rows_seen += 1;
            }
            Ok(())
        })?;
    }
    let rows: Vec<&[Vec<u8>]> = reservoir.iter().map(|row| row.fields.as_slice()).collect();
    write_rows(
        writer,
        schema.header.as_ref().unwrap(),
        col_indices.as_deref(),
        &rows,
        config.table,
        !emit_input_header,
        config.delimiter,
    )
}

fn rank_mode<R: Read, W: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    emit_input_header: bool,
) -> CliResult<()> {
    let rank_cfg = config.rank.clone().unwrap();
    let limit = config.head.unwrap_or(DEFAULT_HEAD_ROWS);
    if limit == 0 {
        return Ok(());
    }
    let mut schema = InputSchema::default();
    let mut col_indices: Option<Vec<usize>> = None;
    let mut rank_col_index: Option<usize> = None;
    let mut ready = false;
    let mut ranked_rows: Vec<RankedRow> = Vec::new();

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, &mut schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !ready {
                col_indices = resolve_output_state(
                    schema.header.as_ref().unwrap(),
                    &config.selectors,
                    &mut config.filters,
                )?;
                rank_col_index = Some(resolve_column_index(
                    schema.header.as_ref().unwrap(),
                    &rank_cfg.field,
                )?);
                ready = true;
            }
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if !passes_filters(&config.filters, &row) {
                    continue;
                }
                let rank_idx = rank_col_index.unwrap();
                let key = row.get(rank_idx).map(|f| f.value.as_slice()).unwrap_or(b"");
                let key_num = parse_f64_bytes(key);
                if ranked_rows.len() < limit {
                    let fields = clone_field_values(&row);
                    let duped_key = fields.get(rank_idx).cloned().unwrap_or_default();
                    ranked_rows.push(RankedRow {
                        fields,
                        key: duped_key,
                        key_num,
                    });
                } else {
                    let wi = worst_rank_index(rank_cfg.direction, &ranked_rows);
                    let worst = &ranked_rows[wi];
                    if compare_rank_keys_for_direction(
                        rank_cfg.direction,
                        key_num,
                        key,
                        worst.key_num,
                        &worst.key,
                    ) == Ordering::Greater
                    {
                        let fields = clone_field_values(&row);
                        let duped_key = fields.get(rank_idx).cloned().unwrap_or_default();
                        ranked_rows[wi] = RankedRow {
                            fields,
                            key: duped_key,
                            key_num,
                        };
                    }
                }
            }
            Ok(())
        })?;
    }
    ranked_rows.sort_by(|a, b| {
        compare_rank_keys_for_direction(rank_cfg.direction, b.key_num, &b.key, a.key_num, &a.key)
    });
    let rows: Vec<&[Vec<u8>]> = ranked_rows
        .iter()
        .map(|row| row.fields.as_slice())
        .collect();
    write_rows(
        writer,
        schema.header.as_ref().unwrap(),
        col_indices.as_deref(),
        &rows,
        config.table,
        !emit_input_header,
        config.delimiter,
    )
}

fn agg_mode<R: Read, W: Write, E: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    stderr: &mut E,
) -> CliResult<()> {
    let mut schema = InputSchema::default();
    let mut ready = false;
    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, &mut schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !ready {
                for agg in &mut config.aggs {
                    agg.col_index = Some(resolve_column_index(
                        schema.header.as_ref().unwrap(),
                        &agg.field,
                    )?);
                }
                for filter in &mut config.filters {
                    filter.col_index = Some(resolve_column_index(
                        schema.header.as_ref().unwrap(),
                        &filter.field,
                    )?);
                }
                ready = true;
            }
            while let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if !passes_filters(&config.filters, &row) {
                    continue;
                }
                for agg in &mut config.aggs {
                    if let Some(col) = agg.col_index {
                        if let Some(field) = row.get(col) {
                            update_agg(agg, &field.value);
                        }
                    }
                }
            }
            Ok(())
        })?;
    }

    let headers: Vec<Vec<u8>> = config
        .aggs
        .iter()
        .map(|agg| format!("{}({})", agg.func.name(), agg.field).into_bytes())
        .collect();
    let mut values = Vec::with_capacity(config.aggs.len());
    for agg in &config.aggs {
        if agg.func == AggFunc::Count {
            values.push(agg.n.to_string().into_bytes());
        } else if agg.tainted {
            writeln!(
                stderr,
                "Warning: {}({}): non-numeric values encountered",
                agg.func.name(),
                agg.field
            )?;
            values.push(Vec::new());
        } else {
            values.push(format_number(agg_result(agg)).into_bytes());
        }
    }
    if config.table {
        let widths: Vec<usize> = headers
            .iter()
            .zip(&values)
            .map(|(h, v)| display_width(h).max(display_width(v)))
            .collect();
        if !config.no_header {
            write_table_row(writer, &headers, None, &widths)?;
            write_table_separator(writer, &widths)?;
        }
        write_table_row(writer, &values, None, &widths)?;
    } else {
        if !config.no_header {
            write_record(writer, &headers, None, config.delimiter)?;
        }
        write_record(writer, &values, None, config.delimiter)?;
    }
    Ok(())
}

fn table_mode<R: Read, W: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    emit_input_header: bool,
) -> CliResult<()> {
    let mut schema = InputSchema::default();
    let mut col_indices: Option<Vec<usize>> = None;
    let mut ready = false;
    let mut widths: Vec<usize> = Vec::new();
    let mut widths_ready = false;
    let mut header_emitted = false;
    let mut buffered_rows: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut sample_bytes = 0;
    let mut rows_written = 0;

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let Some(mut state) =
                init_input_source(reader, source_name, config, &mut schema, &mut line_buf)?
            else {
                return Ok(());
            };
            if !ready {
                col_indices = resolve_output_state(
                    schema.header.as_ref().unwrap(),
                    &config.selectors,
                    &mut config.filters,
                )?;
                ready = true;
            }
            if !widths_ready {
                widths = initial_widths(schema.header.as_ref().unwrap(), col_indices.as_deref());
                widths_ready = true;
            }

            while sample_bytes < TABLE_SAMPLE_BUDGET
                && !config
                    .head
                    .is_some_and(|limit| buffered_rows.len() >= limit)
            {
                let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                    .map_err(|err| read_error(source_name, state.line_no + 1, err))?
                else {
                    break;
                };
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if !passes_filters(&config.filters, &row) {
                    continue;
                }
                let cloned = clone_field_values(&row);
                sample_bytes += cloned.iter().map(Vec::len).sum::<usize>();
                update_widths(&mut widths, &cloned, col_indices.as_deref());
                buffered_rows.push(cloned);
            }

            if !header_emitted && emit_input_header {
                write_table_row(
                    writer,
                    schema.header.as_ref().unwrap(),
                    col_indices.as_deref(),
                    &widths,
                )?;
                write_table_separator(writer, &widths)?;
                header_emitted = true;
            }

            for row in buffered_rows.drain(..) {
                if config.head.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                write_table_row(writer, &row, col_indices.as_deref(), &widths)?;
                rows_written += 1;
            }

            while !config.head.is_some_and(|limit| rows_written >= limit) {
                let Some(line) = read_data_line(reader, &mut line_buf, &mut state)
                    .map_err(|err| read_error(source_name, state.line_no + 1, err))?
                else {
                    break;
                };
                let row = parse_row_for_source(&line, config, source_name, state.line_no)?;
                if passes_filters(&config.filters, &row) {
                    let cloned = clone_field_values(&row);
                    write_table_row(writer, &cloned, col_indices.as_deref(), &widths)?;
                    rows_written += 1;
                }
            }
            Ok(())
        })?;
    }

    if !header_emitted && emit_input_header {
        write_table_row(
            writer,
            schema.header.as_ref().unwrap(),
            col_indices.as_deref(),
            &widths,
        )?;
        write_table_separator(writer, &widths)?;
    }
    Ok(())
}

fn initial_widths(header: &[Vec<u8>], col_indices: Option<&[usize]>) -> Vec<usize> {
    if let Some(indices) = col_indices {
        indices
            .iter()
            .map(|&idx| header.get(idx).map(|v| display_width(v)).unwrap_or(0))
            .collect()
    } else {
        header.iter().map(|field| display_width(field)).collect()
    }
}

fn update_widths(widths: &mut [usize], row: &[Vec<u8>], col_indices: Option<&[usize]>) {
    if let Some(indices) = col_indices {
        for (i, &idx) in indices.iter().enumerate() {
            if let Some(value) = row.get(idx) {
                widths[i] = widths[i].max(display_width(value));
            }
        }
    } else {
        for (i, value) in row.iter().enumerate().take(widths.len()) {
            widths[i] = widths[i].max(display_width(value));
        }
    }
}

fn clone_field_values(row: &[Field]) -> Vec<Vec<u8>> {
    row.iter().map(|field| field.value.clone()).collect()
}

fn passes_filters(filters: &[Filter], fields: &[Field]) -> bool {
    filters.iter().all(|filter| evaluate_filter(filter, fields))
}

fn evaluate_filter(filter: &Filter, fields: &[Field]) -> bool {
    let Some(col_index) = filter.col_index else {
        return true;
    };
    let Some(field) = fields.get(col_index) else {
        return false;
    };
    let field_val = String::from_utf8_lossy(&field.value);
    if filter.op == FilterOp::Like {
        return glob_match(filter.value.as_bytes(), &field.value);
    }
    if let Some(b) = filter.value_num {
        let Ok(a) = field_val.parse::<f64>() else {
            return false;
        };
        return match filter.op {
            FilterOp::Eq => a == b,
            FilterOp::Neq => a != b,
            FilterOp::Lt => a < b,
            FilterOp::Gt => a > b,
            FilterOp::Lte => a <= b,
            FilterOp::Gte => a >= b,
            FilterOp::Like => unreachable!(),
        };
    }
    match field.value.as_slice().cmp(filter.value.as_bytes()) {
        Ordering::Equal => matches!(filter.op, FilterOp::Eq | FilterOp::Lte | FilterOp::Gte),
        Ordering::Less => matches!(filter.op, FilterOp::Neq | FilterOp::Lt | FilterOp::Lte),
        Ordering::Greater => matches!(filter.op, FilterOp::Neq | FilterOp::Gt | FilterOp::Gte),
    }
}

fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut px = 0;
    let mut tx = 0;
    let mut star_px = None;
    let mut star_tx = 0;
    while tx < text.len() || px < pattern.len() {
        if px < pattern.len() && pattern[px] == b'*' {
            star_px = Some(px);
            star_tx = tx;
            px += 1;
        } else if px < pattern.len() && tx < text.len() && pattern[px] == text[tx] {
            px += 1;
            tx += 1;
        } else if let Some(sp) = star_px {
            star_tx += 1;
            if star_tx > text.len() {
                return false;
            }
            tx = star_tx;
            px = sp + 1;
        } else {
            return false;
        }
    }
    true
}

fn compare_rank_keys(a_num: Option<f64>, a: &[u8], b_num: Option<f64>, b: &[u8]) -> Ordering {
    if let (Some(a), Some(b)) = (a_num, b_num) {
        a.partial_cmp(&b).unwrap_or(Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

fn compare_rank_keys_for_direction(
    direction: RankDirection,
    a_num: Option<f64>,
    a: &[u8],
    b_num: Option<f64>,
    b: &[u8],
) -> Ordering {
    let base = compare_rank_keys(a_num, a, b_num, b);
    match direction {
        RankDirection::Greatest => base,
        RankDirection::Least => base.reverse(),
    }
}

fn worst_rank_index(direction: RankDirection, rows: &[RankedRow]) -> usize {
    let mut worst = 0;
    for i in 1..rows.len() {
        if compare_rank_keys_for_direction(
            direction,
            rows[i].key_num,
            &rows[i].key,
            rows[worst].key_num,
            &rows[worst].key,
        ) == Ordering::Less
        {
            worst = i;
        }
    }
    worst
}

fn parse_f64_bytes(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse::<f64>().ok()
}

fn update_agg(agg: &mut Agg, field_val: &[u8]) {
    match agg.func {
        AggFunc::Count => {
            if !field_val.is_empty() {
                agg.n += 1;
            }
        }
        AggFunc::Sum | AggFunc::Mean => match parse_f64_bytes(field_val) {
            Some(v) => {
                agg.total += v;
                agg.n += 1;
            }
            None => agg.tainted = true,
        },
        AggFunc::Min => match parse_f64_bytes(field_val) {
            Some(v) => {
                if agg.n == 0 || v < agg.extreme {
                    agg.extreme = v;
                }
                agg.n += 1;
            }
            None => agg.tainted = true,
        },
        AggFunc::Max => match parse_f64_bytes(field_val) {
            Some(v) => {
                if agg.n == 0 || v > agg.extreme {
                    agg.extreme = v;
                }
                agg.n += 1;
            }
            None => agg.tainted = true,
        },
    }
}

fn agg_result(agg: &Agg) -> f64 {
    match agg.func {
        AggFunc::Sum => agg.total,
        AggFunc::Mean => {
            if agg.n > 0 {
                agg.total / agg.n as f64
            } else {
                0.0
            }
        }
        AggFunc::Min | AggFunc::Max => agg.extreme,
        AggFunc::Count => agg.n as f64,
    }
}

fn format_number(n: f64) -> String {
    n.to_string()
}

fn display_width(bytes: &[u8]) -> usize {
    let mut width = 0;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        width += 1;
        if byte < 0x80 || byte < 0xC0 {
            i += 1;
        } else if byte < 0xE0 {
            i += 2;
        } else if byte < 0xF0 {
            i += 3;
        } else {
            i += 4;
        }
    }
    width
}

fn write_field<W: Write>(writer: &mut W, field: &[u8], delimiter: u8) -> CliResult<()> {
    let needs_quoting = field
        .iter()
        .any(|&c| c == b'"' || c == b'\n' || c == b'\r' || c == delimiter);
    if needs_quoting {
        writer.write_all(b"\"")?;
        for &c in field {
            if c == b'"' {
                writer.write_all(b"\"\"")?;
            } else {
                writer.write_all(&[c])?;
            }
        }
        writer.write_all(b"\"")?;
    } else {
        writer.write_all(field)?;
    }
    Ok(())
}

fn write_record<W: Write>(
    writer: &mut W,
    fields: &[Vec<u8>],
    col_indices: Option<&[usize]>,
    delimiter: u8,
) -> CliResult<()> {
    if let Some(indices) = col_indices {
        for (i, &idx) in indices.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            if let Some(field) = fields.get(idx) {
                write_field(writer, field, delimiter)?;
            }
        }
    } else {
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            write_field(writer, field, delimiter)?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_fields<W: Write>(
    writer: &mut W,
    fields: &[Field],
    col_indices: Option<&[usize]>,
    delimiter: u8,
) -> CliResult<()> {
    let write_one = |writer: &mut W, field: &Field| -> CliResult<()> {
        if field.quoted {
            write_field(writer, &field.value, delimiter)
        } else {
            writer.write_all(&field.value)?;
            Ok(())
        }
    };
    if let Some(indices) = col_indices {
        for (i, &idx) in indices.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            if let Some(field) = fields.get(idx) {
                write_one(writer, field)?;
            }
        }
    } else {
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            write_one(writer, field)?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_rows<W: Write>(
    writer: &mut W,
    header: &[Vec<u8>],
    col_indices: Option<&[usize]>,
    rows: &[&[Vec<u8>]],
    table: bool,
    no_header: bool,
    delimiter: u8,
) -> CliResult<()> {
    if table {
        let mut widths = initial_widths(header, col_indices);
        for row in rows {
            update_widths(&mut widths, row, col_indices);
        }
        if !no_header {
            write_table_row(writer, header, col_indices, &widths)?;
            write_table_separator(writer, &widths)?;
        }
        for row in rows {
            write_table_row(writer, row, col_indices, &widths)?;
        }
    } else {
        if !no_header {
            write_record(writer, header, col_indices, delimiter)?;
        }
        for row in rows {
            write_record(writer, row, col_indices, delimiter)?;
        }
    }
    Ok(())
}

fn write_table_separator<W: Write>(writer: &mut W, widths: &[usize]) -> CliResult<()> {
    for (i, &w) in widths.iter().enumerate() {
        if i > 0 {
            writer.write_all(b"-+-")?;
        }
        for _ in 0..w {
            writer.write_all(b"-")?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_table_row<W: Write>(
    writer: &mut W,
    fields: &[Vec<u8>],
    col_indices: Option<&[usize]>,
    widths: &[usize],
) -> CliResult<()> {
    if let Some(indices) = col_indices {
        for (i, &idx) in indices.iter().enumerate() {
            if i > 0 {
                writer.write_all(b" | ")?;
            }
            let val = fields.get(idx).map(Vec::as_slice).unwrap_or(b"");
            writer.write_all(val)?;
            for _ in display_width(val)..widths[i] {
                writer.write_all(b" ")?;
            }
        }
    } else {
        for (i, &w) in widths.iter().enumerate() {
            if i > 0 {
                writer.write_all(b" | ")?;
            }
            let val = fields.get(i).map(Vec::as_slice).unwrap_or(b"");
            writer.write_all(val)?;
            for _ in display_width(val)..w {
                writer.write_all(b" ")?;
            }
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn values(row: &[Field]) -> Vec<String> {
        row.iter()
            .map(|f| String::from_utf8_lossy(&f.value).into_owned())
            .collect()
    }

    #[test]
    fn parse_record_simple_unquoted_fields() {
        let row = parse_record(b"alice,30,Engineering", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["alice", "30", "Engineering"]);
    }

    #[test]
    fn parse_record_empty_fields() {
        let row = parse_record(b",a,,b,", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["", "a", "", "b", ""]);
    }

    #[test]
    fn parse_record_quoted_and_escaped() {
        let row = parse_record(b"\"she said \"\"hi\"\"\",ok", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["she said \"hi\"", "ok"]);
        assert!(row[0].quoted);
    }

    #[test]
    fn parse_record_custom_delimiter() {
        let row = parse_record(b"alice\t\"sales, west\"\t42", b'\t').unwrap();
        assert_eq!(values(&row), ["alice", "sales, west", "42"]);
    }

    #[test]
    fn parse_record_errors() {
        assert_eq!(
            parse_record(b"\"x\"oops,2", DEFAULT_DELIMITER),
            Err(ParseRecordError::MalformedQuotedField)
        );
        assert_eq!(
            parse_record(b"\"abc,def", DEFAULT_DELIMITER),
            Err(ParseRecordError::UnterminatedQuote)
        );
        let mut line = Vec::new();
        for i in 0..=MAX_FIELDS {
            if i > 0 {
                line.push(b',');
            }
            line.push(b'x');
        }
        assert_eq!(
            parse_record(&line, DEFAULT_DELIMITER),
            Err(ParseRecordError::TooManyFields)
        );
    }

    #[test]
    fn parse_filter_operators_and_whitespace() {
        let f = parse_filter("Total Amount > 0.1").unwrap();
        assert_eq!(f.field, "Total Amount");
        assert_eq!(f.op, FilterOp::Gt);
        assert_eq!(f.value, "0.1");
        assert_eq!(parse_filter("age != 30").unwrap().op, FilterOp::Neq);
        assert_eq!(parse_filter("score<=50").unwrap().op, FilterOp::Lte);
        assert_eq!(parse_filter("name~A*").unwrap().op, FilterOp::Like);
        assert!(parse_filter("").is_none());
        assert!(parse_filter("justtext").is_none());
        assert!(parse_filter("=value").is_none());
    }

    #[test]
    fn parse_args_group_by_removed() {
        assert!(parse_args_list(&args(&["zsv", "--group-by", "dept"])).is_err());
    }

    #[test]
    fn parse_args_head_tail_rank_sample_validate() {
        match parse_args_list(&args(&["zsv", "-n", "-t"])).unwrap() {
            ParsedArgs::Config(cfg) => {
                assert_eq!(cfg.head, Some(DEFAULT_HEAD_ROWS));
                assert!(cfg.table);
            }
            ParsedArgs::Help => panic!(),
        }
        match parse_args_list(&args(&["zsv", "--tail", "-s", "name,score"])).unwrap() {
            ParsedArgs::Config(cfg) => {
                assert_eq!(cfg.tail, Some(DEFAULT_HEAD_ROWS));
                assert_eq!(cfg.selectors, ["name", "score"]);
            }
            ParsedArgs::Help => panic!(),
        }
        assert!(parse_args_list(&args(&["zsv", "--greatest", "salary", "-n", "10001"])).is_err());
        assert!(parse_args_list(&args(&["zsv", "--sample", "0"])).is_err());
        assert!(parse_args_list(&args(&["zsv", "--validate", "--greatest", "x"])).is_err());
    }

    #[test]
    fn glob_match_cases() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(glob_match(b"*world", b"hello world"));
        assert!(glob_match(b"hello*", b"hello world"));
        assert!(glob_match(b"*ell*", b"hello"));
        assert!(glob_match(b"h*l*o", b"hello"));
        assert!(glob_match(b"*", b""));
        assert!(!glob_match(b"", b"notempty"));
    }

    #[test]
    fn evaluate_filter_numeric_and_string() {
        let fields = parse_record(b"Alice,150000,Engineering", DEFAULT_DELIMITER).unwrap();
        let mut f = parse_filter("salary>100000").unwrap();
        f.col_index = Some(1);
        assert!(evaluate_filter(&f, &fields));
        f.value = "200000".to_string();
        f.value_num = Some(200000.0);
        assert!(!evaluate_filter(&f, &fields));

        let mut f = parse_filter("dept=Engineering").unwrap();
        f.col_index = Some(2);
        assert!(evaluate_filter(&f, &fields));
    }

    #[test]
    fn rank_helpers() {
        assert_eq!(
            compare_rank_keys(Some(5.0), b"5", Some(3.0), b"3"),
            Ordering::Greater
        );
        assert_eq!(compare_rank_keys(None, b"b", None, b"a"), Ordering::Greater);
        assert_eq!(
            compare_rank_keys(Some(10.0), b"10", None, b"9"),
            Ordering::Less
        );
        let rows = vec![
            RankedRow {
                fields: vec![],
                key: b"10".to_vec(),
                key_num: Some(10.0),
            },
            RankedRow {
                fields: vec![],
                key: b"3".to_vec(),
                key_num: Some(3.0),
            },
            RankedRow {
                fields: vec![],
                key: b"7".to_vec(),
                key_num: Some(7.0),
            },
        ];
        assert_eq!(worst_rank_index(RankDirection::Greatest, &rows), 1);
        assert_eq!(worst_rank_index(RankDirection::Least, &rows), 0);
    }

    #[test]
    fn display_width_multibyte() {
        assert_eq!(display_width(b"hello"), 5);
        assert_eq!(display_width("··".as_bytes()), 2);
        assert_eq!(display_width("€".as_bytes()), 1);
        assert_eq!(display_width("360 Checking ··4926".as_bytes()), 19);
        assert_eq!(display_width("😀".as_bytes()), 1);
    }

    #[test]
    fn write_field_and_record() {
        let mut out = Vec::new();
        write_field(&mut out, b"she said \"hi\"", DEFAULT_DELIMITER).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "\"she said \"\"hi\"\"\"");
        let mut out = Vec::new();
        write_record(
            &mut out,
            &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            Some(&[2, 0]),
            DEFAULT_DELIMITER,
        )
        .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "c,a\n");
    }

    #[test]
    fn aggregate_helpers() {
        let mut agg = Agg::new(AggFunc::Sum, "x");
        update_agg(&mut agg, b"10");
        update_agg(&mut agg, b"20.5");
        assert_eq!(agg.n, 2);
        assert!((agg.total - 30.5).abs() < 1e-9);
        update_agg(&mut agg, b"not_a_number");
        assert!(agg.tainted);
        assert_eq!(parse_agg("sum:Rate:2024").unwrap().field, "Rate:2024");
        assert!(parse_agg("avg:salary").is_none());
    }

    #[test]
    fn run_select_filter() {
        let input = b"name,score,dept\nAlice,9,Eng\nBob,8,Sales\nCara,10,Eng\n";
        let mut stdin = &input[..];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            &args(&["zsv", "-s", "name,score", "-f", "dept=Eng"]),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "name,score\nAlice,9\nCara,10\n"
        );
    }

    #[test]
    fn run_table_streams_stdin_once() {
        let mut input = b"name,score\nAlice,9\nBob,8\nCara,10\n".as_slice();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(&args(&["zsv", "-t"]), &mut input, &mut stdout, &mut stderr).unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "name  | score\n------+------\nAlice | 9    \nBob   | 8    \nCara  | 10   \n"
        );
    }
}
