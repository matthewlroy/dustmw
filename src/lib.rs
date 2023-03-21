use bytes::{Bytes, BytesMut};
use dustcfg::get_env_var;
use futures::{future, SinkExt, StreamExt};
use std::{error::Error, net::SocketAddr};
use tokio::io;
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

/// PUBLIC METHODS

pub async fn dust_db_create(pile_name: String, data: String) -> io::Result<Option<String>> {
    let result = match create(DustDbCreateSchema { pile_name, data }).await {
        Ok(it) => it,
        Err(box_err) => {
            let e_kind = io::ErrorKind::BrokenPipe;
            let e = format!(
                "Error creating db entry due to broken connection: \"{}\"",
                box_err.to_string()
            );
            let error = io::Error::new(e_kind, e);
            return Err(error);
        }
    };

    return Ok(Some(result));

    // let mut parts = result.splitn(2, ' ');

    // match parts.next() {
    //     Some("0") => match parts.next() {
    //         Some(msg) => Ok(Some(String::from(msg))),
    //         None => return Err("CREATE must have a pile name specified".to_owned()),
    //     },
    //     Some("1") => {
    //         let e_kind = io::ErrorKind::InvalidInput;
    //         let e = format!(
    //             "Error creating db entry due to invalid input: \"{}\"",
    //             box_err.to_string()
    //         );
    //         let error = io::Error::new(e_kind, e);
    //         Err(error)
    //     }
    //     Some(_) => {
    //         println!("_!");
    //         Ok("".to_owned())
    //     }
    //     None => {
    //         println!("None!");
    //         Ok("".to_owned())
    //     }
    // }
}

/// END PUBLIC METHODS

struct DustDbCreateSchema {
    pile_name: String,
    data: String,
}

impl DustDbCreateSchema {
    fn serialize_to_str(&self) -> String {
        format!("CREATE {} {}\n", self.pile_name, self.data)
    }
}

async fn create(create_data: DustDbCreateSchema) -> Result<String, Box<dyn Error>> {
    let addr = format!(
        "{}:{}",
        get_env_var("DUST_DB_ADDR"),
        get_env_var("DUST_DB_PORT")
    )
    .parse::<SocketAddr>()?;

    let mut response: BytesMut = Default::default();
    let mut stdout = FramedWrite::new(io::stdout(), BytesCodec::new()); // TODO: supress stdout
    let mut stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.split();
    let mut sink = FramedWrite::new(w, BytesCodec::new());
    let mut stream = FramedRead::new(r, BytesCodec::new())
        .filter_map(|i| -> future::Ready<Option<Bytes>> {
            match i {
                Ok(i) => {
                    response = i.clone();
                    future::ready(Some(i.freeze()))
                }
                Err(e) => {
                    println!("Error: failed to read from socket; {}", e);
                    future::ready(None)
                }
            }
        })
        .map(Ok);

    let msg = Bytes::from(create_data.serialize_to_str());
    match future::join(sink.send(msg), stdout.send_all(&mut stream)).await {
        (Err(e), _) | (_, Err(e)) => Err(e.into()),
        _ => {
            let mut response_str = String::from_utf8(response.to_ascii_lowercase()).unwrap();

            // remove the \n that is added by our tcp server response
            let len = response_str.len();
            response_str.truncate(len - 1);

            Ok(response_str)
        }
    }
}

mod tests {
    #[tokio::test]
    async fn test_dust_db_create() -> Result<(), Box<dyn std::error::Error>> {
        match crate::dust_db_create("users_from_client".to_owned(), "7A".to_owned()).await {
            Ok(_) => Ok(()),
            Err(e) => panic!("{}", e),
        }
    }
}
