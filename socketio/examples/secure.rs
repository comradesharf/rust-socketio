use rust_socketio::ClientBuilder;
use rustls::ClientConfig;
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, pem::PemObject},
};
use std::fs::File;
use std::io::Read;

fn main() {
    // In case a trusted CA is needed that isn't in the trust chain.
    let cert_path = "ca.crt";
    let mut cert_file = File::open(cert_path).expect("Failed to open cert");
    let mut buf = vec![];
    cert_file
        .read_to_end(&mut buf)
        .expect("Failed to read cert");
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&buf) {
        roots
            .add(cert.expect("Failed to parse certificate"))
            .expect("Failed to add root certificate");
    }
    let tls_connector = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let socket = ClientBuilder::new("https://localhost:4200")
        .tls_config(tls_connector)
        // Not strictly required for HTTPS
        .opening_header("HOST", "localhost")
        .on("error", |err, _| eprintln!("Error: {:#?}", err))
        .connect()
        .expect("Connection failed");

    // use the socket

    socket.disconnect().expect("Disconnect failed")
}
