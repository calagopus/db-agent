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
