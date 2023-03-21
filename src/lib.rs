use bytes::Bytes;
use dustcfg::get_env_var;
use futures::{future, SinkExt, StreamExt, TryStreamExt};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::io;
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

/// PUBLIC METHODS

pub async fn dust_db_create(pile_name: String, data: String) -> Result<(), Box<dyn Error>> {
    println!("called create");
    create(DustDbCreateSchema { pile_name, data }).await?;
    println!("returned from internal create");
    Ok(())
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

fn init_response(i: Bytes) -> Arc<Bytes> {
    Arc::new(i)
}

async fn create(create_data: DustDbCreateSchema) -> Result<(), Box<dyn Error>> {
    let addr = format!(
        "{}:{}",
        get_env_var("DUST_DB_ADDR"),
        get_env_var("DUST_DB_PORT")
    )
    .parse::<SocketAddr>()?;

    let resp: Arc<Bytes> = Default::default();

    let mut stdout = FramedWrite::new(io::stdout(), BytesCodec::new());
    let mut stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.split();
    let mut sink = FramedWrite::new(w, BytesCodec::new());
    let mut stream = FramedRead::new(r, BytesCodec::new())
        .filter_map(|i| match i {
            Ok(i) => {
                println!("something happned here? i = {:?}", i);
                let resp = init_response(i.clone().into());
                future::ready(Some(i.freeze()))
            }
            Err(e) => {
                println!("Error: failed to read from socket; {}", e);
                future::ready(None)
            }
        })
        .map(Ok);

    println!("before msg creation");

    let msg = Bytes::from(create_data.serialize_to_str());

    println!("message instantiated = {:?}", msg);

    // sink.send(msg);

    // Ok(())

    match future::join(sink.send(msg), stdout.send_all(&mut stream)).await {
        (Err(e), _) | (_, Err(e)) => Err(e.into()),
        i => {
            println!("return now!");
            println!("static_resp = {:?}", resp);
            Ok(())
        }
    }
}

mod tests {
    #[tokio::test]
    async fn test_dust_db_create() -> Result<(), Box<dyn std::error::Error>> {
        let _ = crate::dust_db_create("users_from_client".to_owned(), "7A".to_owned()).await;
        Ok(())
    }
}
