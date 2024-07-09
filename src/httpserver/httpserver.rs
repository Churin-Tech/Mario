use crate::httpserver::routers::router_root;
use axum::Router;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::task::JoinHandle;

pub struct HttpServer {
    pub listener: TcpListener,
    pub router: Router,
}

impl HttpServer {
    pub async fn default() -> Self {
        // let port: u16 = 3000;
        // let addr_ipv4 = net::SocketAddr::from((net::Ipv4Addr::UNSPECIFIED, port));
        // let band_addr = SocketAddr::from(addr_ipv4);

        let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
        let router_root = router_root();
        Self {
            listener,
            router: router_root,
        }
    }
    pub async fn run(self) -> JoinHandle<()> {
        let server = axum::serve(self.listener, self.router.into_make_service());
        let handle = spawn(async {
            server.await.unwrap();
        });
        log::info!("httpserver start");
        return handle;
    }
}
