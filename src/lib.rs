use bytes::{Bytes, BytesMut};
use dustcfg::{encode_utf8_to_hex, get_env_var};
use futures::{future, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{error::Error, net::SocketAddr};
use tokio::io;
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

/// PUBLIC SCHEMAS
#[derive(Serialize, Deserialize)]
pub struct CreateUserSchema {
    pub email: String,
    pub password: String,
}

const USERS_PILE_NAME: &'static str = "users";
/// END PUBLIC SCHEMAS

/// PUBLIC METHODS
pub async fn dust_db_create_user(
    sanitized_create_user_obj: CreateUserSchema,
) -> io::Result<Option<String>> {
    let json_string = serde_json::to_string(&sanitized_create_user_obj)?;
    let hex_data = encode_utf8_to_hex(&json_string);
    dust_db_create(USERS_PILE_NAME.to_owned(), hex_data).await
}

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

    let mut parts = result.splitn(2, ' ');

    match parts.next() {
        Some("0") => match parts.next() {
            Some(msg) => Ok(Some(String::from(msg))),
            None => Ok(None),
        },
        Some("1") => {
            let err_msg = parts.next();

            let e_kind = io::ErrorKind::InvalidInput;
            let e = format!(
                "Error creating db entry due to invalid input: \"{}\"",
                err_msg.unwrap()
            );
            let error = io::Error::new(e_kind, e);
            Err(error)
        }
        Some(_) | None => {
            let e_kind = io::ErrorKind::NotFound;
            let e = "Error reading response back from db, entry might have still been created. . .";
            let error = io::Error::new(e_kind, e);
            Err(error)
        }
    }
}

pub async fn dust_db_health_check() -> io::Result<()> {
    let result = match health_check().await {
        Ok(it) => it,
        Err(box_err) => {
            let e_kind = io::ErrorKind::BrokenPipe;
            let e = format!(
                "Error pinging dust db; broken connection: \"{}\"",
                box_err.to_string()
            );
            let error = io::Error::new(e_kind, e);
            return Err(error);
        }
    };

    let mut parts = result.splitn(2, ' ');

    match parts.next() {
        Some("0") => Ok(()),
        Some(_) | None => {
            let e_kind = io::ErrorKind::NotFound;
            let e = "Error reading response back from db, unsure of db connection health. . .";
            let error = io::Error::new(e_kind, e);
            Err(error)
        }
    }
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

async fn health_check() -> Result<String, Box<dyn Error>> {
    let addr = format!(
        "{}:{}",
        get_env_var("DUST_DB_ADDR"),
        get_env_var("DUST_DB_PORT")
    )
    .parse::<SocketAddr>()?;

    let mut response: BytesMut = Default::default();
    let mut stdout = FramedWrite::new(io::sink(), BytesCodec::new());
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

    let msg = Bytes::from("PING\n");
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

async fn create(create_data: DustDbCreateSchema) -> Result<String, Box<dyn Error>> {
    let addr = format!(
        "{}:{}",
        get_env_var("DUST_DB_ADDR"),
        get_env_var("DUST_DB_PORT")
    )
    .parse::<SocketAddr>()?;

    let mut response: BytesMut = Default::default();
    let mut stdout = FramedWrite::new(io::sink(), BytesCodec::new());
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
    async fn test_dust_db_create() {
        match crate::dust_db_create("users_from_client".to_owned(), "7A".to_owned()).await {
            Ok(opt_str) => {
                // 15th character is always a 4 in UUID v4
                let uuidv4_response = opt_str.unwrap();
                assert_eq!(
                    uuidv4_response.chars().nth(14).unwrap().to_string(),
                    "4".to_string()
                );
            }
            Err(e) => panic!("{}", e),
        };
    }

    #[tokio::test]
    #[should_panic]
    async fn test_dust_db_create_fail() {
        match crate::dust_db_create("users_from_client".to_owned(), "7".to_owned()).await {
            Ok(it) => it,
            Err(e) => panic!("{}", e),
        };
    }

    #[tokio::test]
    async fn test_dust_db_health_check() {
        match crate::dust_db_health_check().await {
            Ok(_) => "",
            Err(e) => panic!("{}", e),
        };
    }

    #[tokio::test]
    async fn test_dust_db_create_user() {
        let _ = match crate::dust_db_create_user(crate::CreateUserSchema {
            email: "matthew@saplink.io".to_owned(),
            password: "$2b$10$4Ga7b2ymZ3/HLfLPYLyxtOGWcLhRckny6zsH/i117btQbdjhnWR7W".to_owned(),
        })
        .await
        {
            Ok(uuid) => {
                let output = std::process::Command::new("cat")
                    .arg(format!("/dust/dustdb/data/users/{}.json", uuid.unwrap()))
                    .output()
                    .unwrap();

                assert_eq!(std::str::from_utf8(&output.stdout).unwrap(), "{\"email\":\"matthew@saplink.io\",\"password\":\"$2b$10$4Ga7b2ymZ3/HLfLPYLyxtOGWcLhRckny6zsH/i117btQbdjhnWR7W\"}");
            }
            Err(e) => panic!("{}", e),
        };
    }
}
