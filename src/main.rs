use std::io::{self, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();

    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let mut error = stderr.lock();

    match zsv::run(&args, &mut input, &mut output, &mut error) {
        Ok(()) => {}
        Err(zsv::CliError::BrokenPipe) => {}
        Err(err) => {
            let _ = writeln!(error, "{err}");
            std::process::exit(1);
        }
    }
}
