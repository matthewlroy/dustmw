use std::io::{self, prelude::*};
use std::net::TcpStream;

use dustcfg::get_env_var;

pub fn connect_to_dust_db() -> std::io::Result<TcpStream> {
    let db_addr: String = get_env_var("DUST_DB_ADDR");
    let db_port: String = get_env_var("DUST_DB_PORT");

    if let Ok(stream) = TcpStream::connect(format!("{}:{}", db_addr, db_port)) {
        Ok(stream)
    } else {
        let e_kind = io::ErrorKind::AddrNotAvailable;
        let e = "Could not connect to TCP stream".to_owned();
        let error = io::Error::new(e_kind, e);
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use crate::connect_to_dust_db;

    #[test]
    fn test_connect_to_dust_db() {}
}
