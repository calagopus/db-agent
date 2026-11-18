use std::{
    io,
    net::{Shutdown, TcpStream},
    os::unix::{
        net::{UnixListener, UnixStream},
        process::CommandExt,
    },
    path::PathBuf,
    process::Command,
    thread,
    time::Duration,
};

const USAGE: &str =
    "usage: relay [--forward <socket path>=<tcp address>]... [-- <command> [args]...]";
const FORWARDED_SIGNALS: [libc::c_int; 7] = [
    libc::SIGTERM,
    libc::SIGINT,
    libc::SIGQUIT,
    libc::SIGHUP,
    libc::SIGUSR1,
    libc::SIGUSR2,
    libc::SIGWINCH,
];
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

struct Forward {
    socket: PathBuf,
    target: String,
}

type Args = (Vec<Forward>, Option<Vec<String>>);

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut forwards = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--forward" => {
                let spec = args.next().ok_or("--forward needs a value")?;
                let (socket, target) = spec
                    .rsplit_once('=')
                    .filter(|(socket, target)| !socket.is_empty() && !target.is_empty())
                    .ok_or_else(|| format!("invalid forward {spec:?}"))?;
                forwards.push(Forward {
                    socket: socket.into(),
                    target: target.to_string(),
                });
            }
            "--" => {
                let command: Vec<String> = args.collect();
                if command.is_empty() {
                    return Err("missing command after --".to_string());
                }
                return Ok((forwards, Some(command)));
            }
            other => return Err(format!("unexpected argument {other:?}")),
        }
    }

    if forwards.is_empty() {
        return Err("nothing to do".to_string());
    }
    Ok((forwards, None))
}

fn signal_set() -> libc::sigset_t {
    // SAFETY: sigemptyset initializes the set before sigaddset touches it
    unsafe {
        let mut set = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for signal in FORWARDED_SIGNALS {
            libc::sigaddset(&mut set, signal);
        }
        libc::sigaddset(&mut set, libc::SIGCHLD);
        set
    }
}

fn serve(forward: Forward) {
    while TcpStream::connect(&forward.target).is_err() {
        thread::sleep(PROBE_INTERVAL);
    }

    let listener = match UnixListener::bind(&forward.socket) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("relay: failed to bind {}: {err}", forward.socket.display());
            return;
        }
    };

    for client in listener.incoming().flatten() {
        let target = forward.target.clone();
        thread::spawn(move || {
            if let Err(err) = relay(client, &target) {
                eprintln!("relay: failed to reach {target}: {err}");
            }
        });
    }
}

fn relay(client: UnixStream, target: &str) -> io::Result<()> {
    let backend = TcpStream::connect(target)?;
    let (client_rx, backend_tx) = (client.try_clone()?, backend.try_clone()?);

    let upstream = thread::spawn(move || {
        io::copy(&mut &client_rx, &mut &backend_tx).ok();
        backend_tx.shutdown(Shutdown::Write).ok();
    });

    io::copy(&mut &backend, &mut &client).ok();
    client.shutdown(Shutdown::Both).ok();
    backend.shutdown(Shutdown::Both).ok();
    upstream.join().ok();

    Ok(())
}

fn exit(sockets: &[PathBuf], code: i32) -> ! {
    for socket in sockets {
        std::fs::remove_file(socket).ok();
    }
    std::process::exit(code)
}

fn main() {
    let (forwards, command) = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(err) => {
            eprintln!("relay: {err}\n{USAGE}");
            std::process::exit(2);
        }
    };

    let set = signal_set();
    // SAFETY: set is initialized, the mask is applied before any other thread exists so every
    // thread inherits it and signals are only taken by the sigwait below
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) };

    let sockets: Vec<PathBuf> = forwards.iter().map(|f| f.socket.clone()).collect();
    for socket in &sockets {
        std::fs::remove_file(socket).ok();
    }
    for forward in forwards {
        thread::spawn(move || serve(forward));
    }

    let child = command.and_then(|command| {
        let (program, args) = command.split_first()?;
        let mut cmd = Command::new(program);
        cmd.args(args);
        // SAFETY: pthread_sigmask is async-signal-safe and set is a plain copied value
        unsafe {
            cmd.pre_exec(move || {
                libc::pthread_sigmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
                Ok(())
            });
        }

        match cmd.spawn() {
            Ok(child) => Some(child.id() as libc::pid_t),
            Err(err) => {
                eprintln!("relay: failed to start {program}: {err}");
                exit(&sockets, 127);
            }
        }
    });

    loop {
        let mut signal = 0;
        // SAFETY: set is initialized and signal is a valid out pointer
        if unsafe { libc::sigwait(&set, &mut signal) } != 0 {
            continue;
        }

        if signal == libc::SIGCHLD {
            loop {
                let mut status = 0;
                // SAFETY: status is a valid out pointer
                let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
                if pid <= 0 {
                    break;
                }
                if Some(pid) == child {
                    let code = if libc::WIFSIGNALED(status) {
                        128 + libc::WTERMSIG(status)
                    } else {
                        libc::WEXITSTATUS(status)
                    };
                    exit(&sockets, code);
                }
            }
            continue;
        }

        match child {
            // SAFETY: kill has no memory safety requirements
            Some(pid) => unsafe {
                libc::kill(pid, signal);
            },
            None if signal != libc::SIGWINCH => exit(&sockets, 0),
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, String> {
        parse_args(args.iter().map(|a| a.to_string()))
    }

    // parse_args

    #[test]
    fn parse_args_collects_forwards_split_on_last_equals() {
        let (forwards, command) = parse(&[
            "--forward",
            "/run/a=b.sock=127.0.0.1:3306",
            "--forward",
            "/run/c.sock=[::1]:5432",
        ])
        .expect("valid forwards");

        let pairs: Vec<_> = forwards
            .iter()
            .map(|f| (f.socket.clone(), f.target.as_str()))
            .collect();
        assert_eq!(
            pairs,
            [
                (PathBuf::from("/run/a=b.sock"), "127.0.0.1:3306"),
                (PathBuf::from("/run/c.sock"), "[::1]:5432"),
            ]
        );
        assert_eq!(command, None);
    }

    #[test]
    fn parse_args_passes_command_through_verbatim() {
        let (forwards, command) = parse(&[
            "--forward",
            "/s=h:1",
            "--",
            "mysqld",
            "--forward",
            "x",
            "--",
        ])
        .expect("forward with command");
        assert_eq!(forwards.len(), 1);
        assert_eq!(
            command,
            Some(vec![
                "mysqld".into(),
                "--forward".into(),
                "x".into(),
                "--".into()
            ])
        );

        let (forwards, command) = parse(&["--", "mysqld"]).expect("command alone");
        assert!(forwards.is_empty());
        assert_eq!(command, Some(vec!["mysqld".into()]));
    }

    #[test]
    fn parse_args_rejects_invalid_input() {
        for args in [
            &[][..],
            &["--forward"],
            &["--forward", "no-equals"],
            &["--forward", "=h:1"],
            &["--forward", "/s="],
            &["--forward", "/s=h:1", "--"],
            &["--bogus"],
            &["mysqld"],
        ] {
            assert!(parse(args).is_err(), "{args:?} should be rejected");
        }
    }
}
