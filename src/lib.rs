use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
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
}

#[derive(Clone, Copy, Debug, Default)]
struct AggState {
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
        }
    }

    fn count_all(&self) -> bool {
        self.func == AggFunc::Count && self.field.is_empty()
    }

    fn header_name(&self) -> Vec<u8> {
        if self.count_all() {
            b"count".to_vec()
        } else {
            format!("{}({})", self.func.name(), self.field).into_bytes()
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
    group_by: Option<String>,
}

/// One parsed CSV row. Field bytes (quotes stripped, `""` unescaped) are packed
/// into `data`; reusing a `Record` across rows means parsing does not allocate.
#[derive(Debug, Default, PartialEq, Eq)]
struct Record {
    data: Vec<u8>,
    fields: Vec<FieldSpan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FieldSpan {
    start: usize,
    end: usize,
    quoted: bool,
}

impl Record {
    fn clear(&mut self) {
        self.data.clear();
        self.fields.clear();
    }

    fn len(&self) -> usize {
        self.fields.len()
    }

    fn get(&self, i: usize) -> Option<&[u8]> {
        self.fields.get(i).map(|f| &self.data[f.start..f.end])
    }

    fn quoted(&self, i: usize) -> bool {
        self.fields.get(i).is_some_and(|f| f.quoted)
    }

    fn values(&self) -> impl Iterator<Item = &[u8]> {
        self.fields.iter().map(|f| &self.data[f.start..f.end])
    }

    fn to_vecs(&self) -> Vec<Vec<u8>> {
        self.values().map(<[u8]>::to_vec).collect()
    }

    /// Copies the fields into `out`, reusing its existing buffers.
    fn copy_into(&self, out: &mut Vec<Vec<u8>>) {
        out.resize_with(self.len(), Vec::new);
        for (slot, value) in out.iter_mut().zip(self.values()) {
            value.clone_into(slot);
        }
    }

    /// Closes the field whose bytes start at `start` and end at the current end of `data`.
    fn end_field(&mut self, start: usize, quoted: bool) -> Result<(), ParseRecordError> {
        if self.fields.len() >= MAX_FIELDS {
            return Err(ParseRecordError::TooManyFields);
        }
        self.fields.push(FieldSpan {
            start,
            end: self.data.len(),
            quoted,
        });
        Ok(())
    }
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

#[derive(Debug)]
struct RankedRow {
    fields: Vec<Vec<u8>>,
    key: Vec<u8>,
    key_num: Option<f64>,
    seq: usize,
    direction: RankDirection,
}

/// Greater means ranked higher. Equal keys rank by input order (earlier first),
/// so the kept rows match a stable sort of all rows truncated to N.
impl Ord for RankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rank_keys_for_direction(
            self.direction,
            self.key_num,
            &self.key,
            other.key_num,
            &other.key,
        )
        .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for RankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RankedRow {}

#[derive(Debug)]
struct SampleRow {
    fields: Vec<Vec<u8>>,
}

#[derive(Default)]
struct InputSchema {
    header: Option<Vec<Vec<u8>>>,
    cols: usize,
}

struct SourceState {
    /// With --input-no-header the first line is data: `init_input_source` leaves it
    /// in the line buffer and `read_data_line` returns it before reading more.
    pending_first_data: bool,
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

    /// Returns the cleared buffer for the next row, evicting the oldest row when
    /// full. Slots keep their capacity, so steady-state tailing does not allocate.
    fn next_slot(&mut self) -> &mut Vec<u8> {
        let idx = if self.count < self.capacity {
            let idx = (self.start + self.count) % self.capacity;
            self.count += 1;
            idx
        } else {
            let idx = self.start;
            self.start = (self.start + 1) % self.capacity;
            idx
        };
        let slot = &mut self.rows[idx];
        slot.clear();
        slot
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
    "Usage: zsv [OPTIONS]\n       zsv [OPTIONS] [FILE...]\n\nReads CSV from stdin and writes to stdout.\nIf FILEs are provided, they are processed in order and stacked as one CSV.\nUse - to read from stdin in a file list.\n\nOptions:\n  -s, --select FIELDS   Comma-separated column names or 1-based indices\n  -f, --filter EXPR     Filter expression: field op value\n                        Operators: =, !=, <, >, <=, >=, ~ (glob)\n                        Repeatable (multiple filters = AND)\n  -d, --delimiter DELIM Field delimiter (default comma; supports tab or \\t)\n  -n, --head [N]        Output first N data rows (after filtering; default 10 when omitted)\n      --tail [N]        Output last N data rows (after filtering; preserves header; default 10 when omitted)\n      --greatest FIELD  Output rows with the largest values in FIELD; use -n for count (default 10; max 10000)\n      --least FIELD     Output rows with the smallest values in FIELD; use -n for count (default 10; max 10000)\n      --sample N        Output uniform random sample of N rows (after filtering)\n      --agg FUNC:FIELD  Aggregate FIELD; FUNC: sum, min, max, count, mean\n                        Use --agg count (no field) to count all rows\n                        Repeatable; incompatible with --greatest/--least and --head\n      --group-by FIELD  Group aggregations by FIELD (requires --agg)\n                        Memory grows with number of distinct group values\n  -t, --table           Pretty-print output as an aligned table\n      --no-header       Suppress header row in output\n      --input-no-header Treat the first input row as data\n      --validate        Validate CSV structure (parse + column count)\n  -h, --help            Print this help message\n"
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
        } else if !positional_mode && arg == "--group-by" {
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                CliError::Message("Error: --group-by requires a field name".to_string())
            })?;
            config.group_by = Some(value.clone());
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

    if config.group_by.is_some() && config.aggs.is_empty() {
        return Err(CliError::Message(
            "Error: --group-by requires at least one --agg".to_string(),
        ));
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
    if expr == "count" {
        return Some(Agg::new(AggFunc::Count, ""));
    }
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

fn parse_record(line: &[u8], delimiter: u8, record: &mut Record) -> Result<(), ParseRecordError> {
    record.clear();
    let mut i = 0;
    while i <= line.len() {
        let start = record.data.len();
        if i == line.len() {
            if line.last() == Some(&delimiter) {
                record.end_field(start, false)?;
            }
            break;
        }
        if line[i] == b'"' {
            i += 1;
            loop {
                let Some(q) = line[i..].iter().position(|&c| c == b'"') else {
                    return Err(ParseRecordError::UnterminatedQuote);
                };
                record.data.extend_from_slice(&line[i..i + q]);
                i += q + 1;
                if line.get(i) == Some(&b'"') {
                    record.data.push(b'"');
                    i += 1;
                } else {
                    break;
                }
            }
            record.end_field(start, true)?;
            if i == line.len() {
                break;
            } else if line[i] == delimiter {
                i += 1;
            } else {
                return Err(ParseRecordError::MalformedQuotedField);
            }
        } else {
            let end = line[i..]
                .iter()
                .position(|&c| c == delimiter)
                .map_or(line.len(), |p| i + p);
            record.data.extend_from_slice(&line[i..end]);
            record.end_field(start, false)?;
            if end < line.len() {
                i = end + 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// Reads the next non-empty line into `buf`, without its line terminator.
/// Returns false at end of input.
fn read_next_line<R: BufRead + ?Sized>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<bool> {
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', buf)?;
        if n == 0 {
            return Ok(false);
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
        return Ok(true);
    }
}

fn read_data_line<R: BufRead + ?Sized>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    state: &mut SourceState,
) -> io::Result<bool> {
    if state.pending_first_data {
        state.pending_first_data = false;
        state.line_no += 1;
        return Ok(true);
    }
    let found = read_next_line(reader, buf)?;
    if found {
        state.line_no += 1;
    }
    Ok(found)
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

fn headers_match(expected: &[Vec<u8>], actual: &Record) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual.values())
            .all(|(a, b)| a.as_slice() == b)
}

fn init_input_source<R: BufRead + ?Sized>(
    reader: &mut R,
    source_name: &str,
    config: &Config,
    schema: &mut InputSchema,
    buf: &mut Vec<u8>,
    record: &mut Record,
) -> CliResult<Option<SourceState>> {
    if !read_next_line(reader, buf).map_err(|err| read_error(source_name, 1, err))? {
        return Ok(None);
    }
    parse_record(buf, config.delimiter, record).map_err(|err| {
        CliError::Message(format!(
            "Error parsing CSV in {source_name} on line 1: {}",
            err.message()
        ))
    })?;
    if config.input_no_header {
        if schema.header.is_none() {
            schema.cols = record.len();
            schema.header = Some(make_synthetic_header(record.len()));
        } else if record.len() != schema.cols {
            return Err(CliError::Message(format!(
                "Error: column count mismatch in {source_name}: expected {}, got {}",
                schema.cols,
                record.len()
            )));
        }
        return Ok(Some(SourceState {
            pending_first_data: true,
            line_no: 0,
        }));
    }

    if let Some(header) = &schema.header {
        if !headers_match(header, record) {
            return Err(CliError::Message(format!(
                "Error: header mismatch in {source_name}"
            )));
        }
    } else {
        schema.cols = record.len();
        schema.header = Some(record.to_vecs());
    }
    Ok(Some(SourceState {
        pending_first_data: false,
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
    record: &mut Record,
) -> CliResult<()> {
    parse_record(line, config.delimiter, record).map_err(|err| {
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
        if config.group_by.is_some() {
            grouped_agg_mode(&input_paths, &mut config, stdin, &mut writer, stderr)?;
        } else {
            agg_mode(&input_paths, &mut config, stdin, &mut writer, stderr)?;
        }
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
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                schema,
                &mut line_buf,
                &mut record,
            )?
            else {
                return Ok(());
            };
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if record.len() != schema.cols {
                    return Err(CliError::Message(format!(
                        "Error: column count mismatch in {source_name} on line {}: expected {}, got {}",
                        state.line_no,
                        schema.cols,
                        record.len()
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
            let mut record = Record::default();
            if config.input_no_header {
                while read_next_line(reader, &mut line_buf)
                    .map_err(|err| read_error(source_name, rows_written + 1, err))?
                {
                    if config.head.is_some_and(|limit| rows_written >= limit) {
                        break;
                    }
                    write_raw_line_tail_aware(writer, tail.as_mut(), &line_buf)?;
                    rows_written += 1;
                }
                return Ok(());
            }

            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                schema,
                &mut line_buf,
                &mut record,
            )?
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
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                if config.head.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                write_raw_line_tail_aware(writer, tail.as_mut(), &line_buf)?;
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
        let slot = tail.next_slot();
        slot.extend_from_slice(line);
        slot.push(b'\n');
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
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
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
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                if config.head.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if passes_filters(&config.filters, &record) {
                    write_fields_tail_aware(
                        writer,
                        tail.as_mut(),
                        &record,
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
    record: &Record,
    col_indices: Option<&[usize]>,
    delimiter: u8,
) -> CliResult<()> {
    if let Some(tail) = tail {
        write_fields(tail.next_slot(), record, col_indices, delimiter)?;
    } else {
        write_fields(writer, record, col_indices, delimiter)?;
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
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
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
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if !passes_filters(&config.filters, &record) {
                    continue;
                }
                if reservoir.len() < sample_n {
                    reservoir.push(SampleRow {
                        fields: record.to_vecs(),
                    });
                } else {
                    let j = prng.range_less_than(rows_seen + 1);
                    if j < sample_n {
                        record.copy_into(&mut reservoir[j].fields);
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
    // Min-heap of the best `limit` rows seen so far; the root is the current worst.
    let mut heap: BinaryHeap<Reverse<RankedRow>> = BinaryHeap::with_capacity(limit);
    let mut seq = 0;

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
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
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if !passes_filters(&config.filters, &record) {
                    continue;
                }
                let rank_idx = rank_col_index.unwrap();
                let key = record.get(rank_idx).unwrap_or(b"");
                let key_num = parse_f64_bytes(key);
                if heap.len() < limit {
                    heap.push(Reverse(RankedRow {
                        fields: record.to_vecs(),
                        key: key.to_vec(),
                        key_num,
                        seq,
                        direction: rank_cfg.direction,
                    }));
                } else {
                    let mut worst = heap.peek_mut().expect("heap holds limit > 0 rows");
                    // A later row with an equal key ranks lower, so it must be strictly better.
                    if compare_rank_keys_for_direction(
                        rank_cfg.direction,
                        key_num,
                        key,
                        worst.0.key_num,
                        &worst.0.key,
                    ) == Ordering::Greater
                    {
                        let row = &mut worst.0;
                        record.copy_into(&mut row.fields);
                        row.key.clear();
                        row.key.extend_from_slice(key);
                        row.key_num = key_num;
                        row.seq = seq;
                    }
                }
                seq += 1;
            }
            Ok(())
        })?;
    }
    let ranked_rows = heap.into_sorted_vec();
    let rows: Vec<&[Vec<u8>]> = ranked_rows
        .iter()
        .map(|Reverse(row)| row.fields.as_slice())
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

fn grouped_agg_mode<R: Read, W: Write, E: Write>(
    input_paths: &[String],
    config: &mut Config,
    stdin: &mut R,
    writer: &mut W,
    stderr: &mut E,
) -> CliResult<()> {
    use std::collections::HashMap;

    let mut schema = InputSchema::default();
    let mut ready = false;
    let mut group_col_index: usize = 0;
    let n_aggs = config.aggs.len();
    // Group key -> group number in first-seen order. Accumulators for group g are
    // states[g * n_aggs..(g + 1) * n_aggs]. Each key is stored once.
    let mut groups: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut states: Vec<AggState> = Vec::new();

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
            else {
                return Ok(());
            };
            if !ready {
                group_col_index = resolve_column_index(
                    schema.header.as_ref().unwrap(),
                    config.group_by.as_ref().unwrap(),
                )?;
                for agg in &mut config.aggs {
                    if !agg.count_all() {
                        agg.col_index = Some(resolve_column_index(
                            schema.header.as_ref().unwrap(),
                            &agg.field,
                        )?);
                    }
                }
                ready = true;
            }
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if !passes_filters(&config.filters, &record) {
                    continue;
                }
                let key = record.get(group_col_index).unwrap_or(b"");
                let group = match groups.get(key) {
                    Some(&group) => group,
                    None => {
                        let group = groups.len();
                        groups.insert(key.to_vec(), group);
                        states.resize(states.len() + n_aggs, AggState::default());
                        group
                    }
                };
                let group_states = &mut states[group * n_aggs..(group + 1) * n_aggs];
                for (agg, agg_state) in config.aggs.iter().zip(group_states) {
                    if agg.count_all() {
                        agg_state.n += 1;
                    } else if let Some(col) = agg.col_index {
                        if let Some(value) = record.get(col) {
                            update_agg(agg.func, agg_state, value);
                        }
                    }
                }
            }
            Ok(())
        })?;
    }

    let group_col_name = schema
        .header
        .as_ref()
        .and_then(|h| h.get(group_col_index))
        .cloned()
        .unwrap_or_default();

    let mut headers: Vec<Vec<u8>> = vec![group_col_name];
    headers.extend(config.aggs.iter().map(|agg| agg.header_name()));

    let mut ordered: Vec<(Vec<u8>, usize)> = groups.into_iter().collect();
    ordered.sort_unstable_by_key(|&(_, group)| group);

    if config.table {
        let mut rows: Vec<Vec<Vec<u8>>> = Vec::with_capacity(ordered.len());
        for (key, group) in ordered {
            let group_states = &states[group * n_aggs..(group + 1) * n_aggs];
            rows.push(group_values(key, &config.aggs, group_states, stderr)?);
        }
        let mut widths: Vec<usize> = headers.iter().map(|h| display_width(h)).collect();
        for row in &rows {
            for (i, v) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(display_width(v));
                }
            }
        }
        if !config.no_header {
            write_table_row(writer, &headers, None, &widths)?;
            write_table_separator(writer, &widths)?;
        }
        for row in &rows {
            write_table_row(writer, row, None, &widths)?;
        }
    } else {
        if !config.no_header {
            write_record(writer, &headers, None, config.delimiter)?;
        }
        for (key, group) in ordered {
            let group_states = &states[group * n_aggs..(group + 1) * n_aggs];
            let values = group_values(key, &config.aggs, group_states, stderr)?;
            write_record(writer, &values, None, config.delimiter)?;
        }
    }

    Ok(())
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
    let mut states = vec![AggState::default(); config.aggs.len()];
    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
            else {
                return Ok(());
            };
            if !ready {
                for agg in &mut config.aggs {
                    if !agg.count_all() {
                        agg.col_index = Some(resolve_column_index(
                            schema.header.as_ref().unwrap(),
                            &agg.field,
                        )?);
                    }
                }
                for filter in &mut config.filters {
                    filter.col_index = Some(resolve_column_index(
                        schema.header.as_ref().unwrap(),
                        &filter.field,
                    )?);
                }
                ready = true;
            }
            while read_data_line(reader, &mut line_buf, &mut state)
                .map_err(|err| read_error(source_name, state.line_no + 1, err))?
            {
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if !passes_filters(&config.filters, &record) {
                    continue;
                }
                for (agg, agg_state) in config.aggs.iter().zip(states.iter_mut()) {
                    if agg.count_all() {
                        agg_state.n += 1;
                    } else if let Some(col) = agg.col_index {
                        if let Some(value) = record.get(col) {
                            update_agg(agg.func, agg_state, value);
                        }
                    }
                }
            }
            Ok(())
        })?;
    }

    let headers: Vec<Vec<u8>> = config.aggs.iter().map(|agg| agg.header_name()).collect();
    let mut values = Vec::with_capacity(config.aggs.len());
    for (agg, state) in config.aggs.iter().zip(&states) {
        values.push(format_agg_value(agg, state, stderr)?);
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
    let mut row_buf: Vec<Vec<u8>> = Vec::new();

    for input_path in input_paths {
        with_source(input_path, stdin, |reader, source_name| {
            let mut line_buf = Vec::with_capacity(MAX_LINE_LEN);
            let mut record = Record::default();
            let Some(mut state) = init_input_source(
                reader,
                source_name,
                config,
                &mut schema,
                &mut line_buf,
                &mut record,
            )?
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
                if !read_data_line(reader, &mut line_buf, &mut state)
                    .map_err(|err| read_error(source_name, state.line_no + 1, err))?
                {
                    break;
                }
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if !passes_filters(&config.filters, &record) {
                    continue;
                }
                let cloned = record.to_vecs();
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
                if !read_data_line(reader, &mut line_buf, &mut state)
                    .map_err(|err| read_error(source_name, state.line_no + 1, err))?
                {
                    break;
                }
                parse_row_for_source(&line_buf, config, source_name, state.line_no, &mut record)?;
                if passes_filters(&config.filters, &record) {
                    record.copy_into(&mut row_buf);
                    write_table_row(writer, &row_buf, col_indices.as_deref(), &widths)?;
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

fn passes_filters(filters: &[Filter], record: &Record) -> bool {
    filters.iter().all(|filter| evaluate_filter(filter, record))
}

fn evaluate_filter(filter: &Filter, record: &Record) -> bool {
    let Some(col_index) = filter.col_index else {
        return true;
    };
    let Some(value) = record.get(col_index) else {
        return false;
    };
    if filter.op == FilterOp::Like {
        return glob_match(filter.value.as_bytes(), value);
    }
    if let Some(b) = filter.value_num {
        let Some(a) = parse_f64_bytes(value) else {
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
    match value.cmp(filter.value.as_bytes()) {
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

/// A total order over rank keys; greater means ranked higher. Keys that parse as
/// numbers rank ahead of non-numeric keys in both directions. Within each class,
/// numbers compare with `f64::total_cmp` and text compares as bytes, and `direction`
/// picks which end of that class ranks higher.
fn compare_rank_keys_for_direction(
    direction: RankDirection,
    a_num: Option<f64>,
    a: &[u8],
    b_num: Option<f64>,
    b: &[u8],
) -> Ordering {
    let within_class = |ord: Ordering| match direction {
        RankDirection::Greatest => ord,
        RankDirection::Least => ord.reverse(),
    };
    match (a_num, b_num) {
        (Some(x), Some(y)) => within_class(x.total_cmp(&y)),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => within_class(a.cmp(b)),
    }
}

fn parse_f64_bytes(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse::<f64>().ok()
}

fn update_agg(func: AggFunc, state: &mut AggState, field_val: &[u8]) {
    match func {
        AggFunc::Count => {
            if !field_val.is_empty() {
                state.n += 1;
            }
        }
        AggFunc::Sum | AggFunc::Mean => match parse_f64_bytes(field_val) {
            Some(v) => {
                state.total += v;
                state.n += 1;
            }
            None => state.tainted = true,
        },
        AggFunc::Min => match parse_f64_bytes(field_val) {
            Some(v) => {
                if state.n == 0 || v < state.extreme {
                    state.extreme = v;
                }
                state.n += 1;
            }
            None => state.tainted = true,
        },
        AggFunc::Max => match parse_f64_bytes(field_val) {
            Some(v) => {
                if state.n == 0 || v > state.extreme {
                    state.extreme = v;
                }
                state.n += 1;
            }
            None => state.tainted = true,
        },
    }
}

fn agg_result(func: AggFunc, state: &AggState) -> f64 {
    match func {
        AggFunc::Sum => state.total,
        AggFunc::Mean => {
            if state.n > 0 {
                state.total / state.n as f64
            } else {
                0.0
            }
        }
        AggFunc::Min | AggFunc::Max => state.extreme,
        AggFunc::Count => state.n as f64,
    }
}

fn format_number(n: f64) -> String {
    n.to_string()
}

fn format_agg_value<E: Write>(agg: &Agg, state: &AggState, stderr: &mut E) -> CliResult<Vec<u8>> {
    if agg.func == AggFunc::Count {
        return Ok(state.n.to_string().into_bytes());
    }
    if state.tainted {
        writeln!(
            stderr,
            "Warning: {}({}): non-numeric values encountered",
            agg.func.name(),
            agg.field
        )?;
        return Ok(Vec::new());
    }
    Ok(format_number(agg_result(agg.func, state)).into_bytes())
}

/// Formats one output row of grouped aggregation: the group key, then each aggregate.
fn group_values<E: Write>(
    key: Vec<u8>,
    aggs: &[Agg],
    states: &[AggState],
    stderr: &mut E,
) -> CliResult<Vec<Vec<u8>>> {
    let mut values = Vec::with_capacity(aggs.len() + 1);
    values.push(key);
    for (agg, state) in aggs.iter().zip(states) {
        values.push(format_agg_value(agg, state, stderr)?);
    }
    Ok(values)
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

fn write_repeated<W: Write>(writer: &mut W, byte: u8, count: usize) -> CliResult<()> {
    let chunk = [byte; 64];
    let mut remaining = count;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        writer.write_all(&chunk[..n])?;
        remaining -= n;
    }
    Ok(())
}

fn write_field<W: Write>(writer: &mut W, field: &[u8], delimiter: u8) -> CliResult<()> {
    let needs_quoting = field
        .iter()
        .any(|&c| c == b'"' || c == b'\n' || c == b'\r' || c == delimiter);
    if needs_quoting {
        writer.write_all(b"\"")?;
        for (i, part) in field.split(|&c| c == b'"').enumerate() {
            if i > 0 {
                writer.write_all(b"\"\"")?;
            }
            writer.write_all(part)?;
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
    record: &Record,
    col_indices: Option<&[usize]>,
    delimiter: u8,
) -> CliResult<()> {
    let write_one = |writer: &mut W, idx: usize| -> CliResult<()> {
        let Some(value) = record.get(idx) else {
            return Ok(());
        };
        if record.quoted(idx) {
            write_field(writer, value, delimiter)
        } else {
            writer.write_all(value)?;
            Ok(())
        }
    };
    if let Some(indices) = col_indices {
        for (i, &idx) in indices.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            write_one(writer, idx)?;
        }
    } else {
        for idx in 0..record.len() {
            if idx > 0 {
                writer.write_all(&[delimiter])?;
            }
            write_one(writer, idx)?;
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
        write_repeated(writer, b'-', w)?;
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
            write_repeated(writer, b' ', widths[i].saturating_sub(display_width(val)))?;
        }
    } else {
        for (i, &w) in widths.iter().enumerate() {
            if i > 0 {
                writer.write_all(b" | ")?;
            }
            let val = fields.get(i).map(Vec::as_slice).unwrap_or(b"");
            writer.write_all(val)?;
            write_repeated(writer, b' ', w.saturating_sub(display_width(val)))?;
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

    fn values(row: &Record) -> Vec<String> {
        row.values()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect()
    }

    fn parse(line: &[u8], delimiter: u8) -> Result<Record, ParseRecordError> {
        let mut record = Record::default();
        parse_record(line, delimiter, &mut record).map(|()| record)
    }

    #[test]
    fn parse_record_simple_unquoted_fields() {
        let row = parse(b"alice,30,Engineering", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["alice", "30", "Engineering"]);
    }

    #[test]
    fn parse_record_empty_fields() {
        let row = parse(b",a,,b,", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["", "a", "", "b", ""]);
    }

    #[test]
    fn parse_record_quoted_and_escaped() {
        let row = parse(b"\"she said \"\"hi\"\"\",ok", DEFAULT_DELIMITER).unwrap();
        assert_eq!(values(&row), ["she said \"hi\"", "ok"]);
        assert!(row.quoted(0));
    }

    #[test]
    fn parse_record_custom_delimiter() {
        let row = parse(b"alice\t\"sales, west\"\t42", b'\t').unwrap();
        assert_eq!(values(&row), ["alice", "sales, west", "42"]);
    }

    #[test]
    fn parse_record_errors() {
        assert_eq!(
            parse(b"\"x\"oops,2", DEFAULT_DELIMITER),
            Err(ParseRecordError::MalformedQuotedField)
        );
        assert_eq!(
            parse(b"\"abc,def", DEFAULT_DELIMITER),
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
            parse(&line, DEFAULT_DELIMITER),
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
    fn parse_args_group_by() {
        match parse_args_list(&args(&["zsv", "--group-by", "dept", "--agg", "count"])).unwrap() {
            ParsedArgs::Config(cfg) => {
                assert_eq!(cfg.group_by, Some("dept".to_string()));
                assert_eq!(cfg.aggs.len(), 1);
                assert!(cfg.aggs[0].count_all());
            }
            ParsedArgs::Help => panic!(),
        }
        assert!(parse_args_list(&args(&["zsv", "--group-by", "dept"])).is_err());
    }

    #[test]
    fn run_grouped_agg_count_all() {
        let input = b"dept,name\neng,alice\neng,bob\nops,carol\n";
        let mut stdin = &input[..];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            &args(&["zsv", "--group-by", "dept", "--agg", "count"]),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "dept,count\neng,2\nops,1\n"
        );
    }

    #[test]
    fn run_grouped_agg_sum() {
        let input = b"dept,salary\neng,100\neng,200\nops,50\n";
        let mut stdin = &input[..];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            &args(&["zsv", "--group-by", "dept", "--agg", "sum:salary"]),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "dept,sum(salary)\neng,300\nops,50\n"
        );
    }

    #[test]
    fn run_grouped_agg_multiple() {
        let input = b"dept,salary\neng,100\neng,200\nops,50\n";
        let mut stdin = &input[..];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            &args(&[
                "zsv",
                "--group-by",
                "dept",
                "--agg",
                "count",
                "--agg",
                "mean:salary",
            ]),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "dept,count,mean(salary)\neng,2,150\nops,1,50\n"
        );
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
        let fields = parse(b"Alice,150000,Engineering", DEFAULT_DELIMITER).unwrap();
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
        use RankDirection::{Greatest, Least};
        let cmp = compare_rank_keys_for_direction;
        assert_eq!(
            cmp(Greatest, Some(5.0), b"5", Some(3.0), b"3"),
            Ordering::Greater
        );
        assert_eq!(cmp(Least, Some(5.0), b"5", Some(3.0), b"3"), Ordering::Less);
        assert_eq!(cmp(Greatest, None, b"b", None, b"a"), Ordering::Greater);
        assert_eq!(cmp(Least, None, b"b", None, b"a"), Ordering::Less);
        // A numeric key outranks a non-numeric one in both directions.
        assert_eq!(
            cmp(Greatest, Some(10.0), b"10", None, b"9x"),
            Ordering::Greater
        );
        assert_eq!(
            cmp(Least, Some(10.0), b"10", None, b"9x"),
            Ordering::Greater
        );
        assert_eq!(cmp(Least, None, b"9x", Some(10.0), b"10"), Ordering::Less);
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
        let mut state = AggState::default();
        update_agg(AggFunc::Sum, &mut state, b"10");
        update_agg(AggFunc::Sum, &mut state, b"20.5");
        assert_eq!(state.n, 2);
        assert!((state.total - 30.5).abs() < 1e-9);
        update_agg(AggFunc::Sum, &mut state, b"not_a_number");
        assert!(state.tainted);
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

    #[test]
    fn parse_record_reuses_buffers_between_rows() {
        let mut record = Record::default();
        parse_record(
            b"\"a \"\"long\"\" one\",second,third",
            DEFAULT_DELIMITER,
            &mut record,
        )
        .unwrap();
        parse_record(b"x,\"y\"", DEFAULT_DELIMITER, &mut record).unwrap();
        assert_eq!(values(&record), ["x", "y"]);
        assert!(!record.quoted(0));
        assert!(record.quoted(1));
    }

    #[test]
    fn run_tail_wraps_ring_buffer() {
        let input = b"n,v\n1,a\n2,b\n3,c\n4,d\n5,e\n";
        for (extra, expected) in [
            (&[][..], "n,v\n4,d\n5,e\n"),
            (&["-s", "v"][..], "v\nd\ne\n"),
        ] {
            let mut argv = vec!["zsv", "--tail", "2"];
            argv.extend_from_slice(extra);
            assert_eq!(run_ok(&argv, input), expected);
        }
    }

    #[test]
    fn write_field_escapes_every_quote() {
        let mut out = Vec::new();
        write_field(&mut out, b"\"", DEFAULT_DELIMITER).unwrap();
        assert_eq!(out, b"\"\"\"\"");
        let mut out = Vec::new();
        write_field(&mut out, b"\"a\"\"b\"", DEFAULT_DELIMITER).unwrap();
        assert_eq!(out, b"\"\"\"a\"\"\"\"b\"\"\"");
    }

    fn run_ok(argv: &[&str], input: &[u8]) -> String {
        let mut stdin = input;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(&args(argv), &mut stdin, &mut stdout, &mut stderr).unwrap();
        String::from_utf8(stdout).unwrap()
    }

    #[test]
    fn run_rank_ties_keep_earliest_rows() {
        let input = b"name,score\nA,5\nB,5\nC,6\nD,5\n";
        assert_eq!(
            run_ok(
                &["zsv", "--greatest", "score", "-n", "2", "-s", "name"],
                input
            ),
            "name\nC\nA\n"
        );
        assert_eq!(
            run_ok(&["zsv", "--least", "score", "-n", "2", "-s", "name"], input),
            "name\nA\nB\n"
        );
    }

    #[test]
    fn run_rank_many_rows() {
        let mut input = b"id,v\n".to_vec();
        for i in 0..1000u32 {
            input.extend_from_slice(format!("{i},{}\n", (i * 37) % 1000).as_bytes());
        }
        assert_eq!(
            run_ok(
                &[
                    "zsv",
                    "--greatest",
                    "v",
                    "-n",
                    "5",
                    "-s",
                    "v",
                    "--no-header"
                ],
                &input
            ),
            "999\n998\n997\n996\n995\n"
        );
        assert_eq!(
            run_ok(
                &["zsv", "--least", "v", "-n", "3", "-s", "v", "--no-header"],
                &input
            ),
            "0\n1\n2\n"
        );
    }

    #[test]
    fn run_grouped_agg_first_seen_order_and_taint() {
        let input = b"dept,salary\nops,1\neng,2\nops,x\nhr,4\neng,3\n";
        let mut stdin = &input[..];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            &args(&[
                "zsv",
                "--group-by",
                "dept",
                "--agg",
                "count",
                "--agg",
                "sum:salary",
            ]),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "dept,count,sum(salary)\nops,2,\neng,2,5\nhr,1,4\n"
        );
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "Warning: sum(salary): non-numeric values encountered\n"
        );
    }

    #[test]
    fn run_rank_mixed_numeric_and_text_keys() {
        let input = b"name,score\na,1x\nb,2\nc,10\nd,3\ne,5\n";
        assert_eq!(
            run_ok(
                &["zsv", "--greatest", "score", "-n", "4", "-s", "name"],
                input
            ),
            "name\nc\ne\nd\nb\n"
        );
        assert_eq!(
            run_ok(&["zsv", "--least", "score", "-n", "4", "-s", "name"], input),
            "name\nb\nd\ne\nc\n"
        );
        assert_eq!(
            run_ok(&["zsv", "--least", "score", "-n", "5", "-s", "name"], input),
            "name\nb\nd\ne\nc\na\n"
        );
        let reversed = b"name,score\ne,5\nd,3\nc,10\nb,2\na,1x\n";
        assert_eq!(
            run_ok(
                &["zsv", "--greatest", "score", "-n", "4", "-s", "name"],
                reversed
            ),
            "name\nc\ne\nd\nb\n"
        );
    }

    #[test]
    fn run_rank_nan_key_does_not_panic() {
        let input = b"name,v\na,NaN\nb,1\nc,2\n";
        // total_cmp puts positive NaN above every finite value.
        assert_eq!(
            run_ok(&["zsv", "--greatest", "v", "-n", "3", "-s", "name"], input),
            "name\na\nc\nb\n"
        );
    }
}
