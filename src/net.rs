use std::str::FromStr;

pub fn host_to_ip(host: &str) -> Option<std::net::IpAddr> {
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);

    std::net::IpAddr::from_str(host).ok()
}

pub fn is_blocked_ip(cidrs: &[cidr::IpCidr], ip: &std::net::IpAddr) -> bool {
    let ip = ip.to_canonical();

    cidrs.iter().any(|cidr| cidr.contains(&ip))
}

#[cfg(target_os = "linux")]
pub fn tcp_congestion_control_supported(algorithm: &str) -> bool {
    use std::{collections::HashMap, sync::OnceLock};

    static CACHE: OnceLock<parking_lot::Mutex<HashMap<String, bool>>> = OnceLock::new();

    let cache = CACHE.get_or_init(Default::default);
    if let Some(&supported) = cache.lock().get(algorithm) {
        return supported;
    }

    let available = |algorithm: &str| {
        std::fs::read_to_string("/proc/sys/net/ipv4/tcp_available_congestion_control")
            .is_ok_and(|contents| contents.split_whitespace().any(|entry| entry == algorithm))
    };

    let mut supported = available(algorithm);
    if !supported {
        std::process::Command::new("modprobe")
            .arg(format!("tcp_{algorithm}"))
            .output()
            .ok();

        supported = available(algorithm);
    }

    if !supported {
        tracing::warn!(
            algorithm = %algorithm,
            "the configured tcp congestion control algorithm is not available on this kernel, keeping the system default"
        );
    }

    cache.lock().insert(algorithm.to_string(), supported);

    supported
}

#[cfg(not(target_os = "linux"))]
pub fn tcp_congestion_control_supported(_algorithm: &str) -> bool {
    false
}

#[cfg(target_os = "linux")]
pub fn apply_socket_congestion_control<F: std::os::fd::AsFd>(
    listener: &F,
    config: &crate::config::Config,
) {
    let algorithm = config.load().tcp_congestion_control.clone();
    if algorithm.is_empty() || !tcp_congestion_control_supported(&algorithm) {
        return;
    }

    if let Err(err) = rustix::net::sockopt::set_tcp_congestion(listener, &algorithm) {
        tracing::debug!(
            algorithm = %algorithm,
            "failed to set tcp congestion control on listener: {}",
            err
        );
    }
}

#[cfg(not(target_os = "linux"))]
pub fn apply_socket_congestion_control<F>(_listener: &F, _config: &crate::config::Config) {}
