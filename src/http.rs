use crate::structs::WmInnerSignals;
#[cfg(feature = "ota")]
use alloc::format;
use alloc::rc::Rc;
use embassy_executor::Spawner;
use embassy_net::{tcp::TcpSocket, Stack};
use embassy_time::{Duration, Timer};
#[cfg(feature = "ota")]
use embedded_io_async::Write;

const WEB_TASK_POOL_SIZE: usize = 2;
const HTTP_BUFFER_SIZE: usize = 2048;
const HTTP_HEADER_BUFFER_SIZE: usize = 192;

struct HttpRequest<'a> {
    method: &'a str,
    path: &'a str,
    headers: &'a [u8],
    body: &'a [u8],
}

fn parse_http_request(buffer: &[u8]) -> Option<HttpRequest<'_>> {
    let header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n")?;

    let header_section = core::str::from_utf8(&buffer[..header_end]).ok()?;

    let mut lines = header_section.lines();
    let first_line = lines.next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;

    let headers_start = header_section.find("\r\n").map(|i| i + 2).unwrap_or(0);
    let headers = &buffer[headers_start..header_end];

    let body = &buffer[header_end + 4..];

    Some(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

async fn write_all_to_socket(socket: &mut TcpSocket<'_>, mut data: &[u8]) -> bool {
    while !data.is_empty() {
        match socket.write(data).await {
            Ok(0) => break,
            Ok(n) => data = &data[n..],
            Err(e) => {
                log::error!("Http wifimanager write error: {e:?}");
                return false;
            }
        }
    }

    _ = socket.flush().await;
    data.is_empty()
}

async fn write_response(
    socket: &mut TcpSocket<'_>,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> bool {
    let mut header: heapless::String<HTTP_HEADER_BUFFER_SIZE> = heapless::String::new();
    _ = core::fmt::write(
        &mut header,
        format_args!(
            "HTTP/1.1 {}\r\nContent-Type: {}; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            status,
            content_type,
            body.len()
        ),
    );

    if !write_all_to_socket(socket, header.as_bytes()).await {
        return false;
    }

    write_all_to_socket(socket, body).await
}

#[cfg(feature = "ota")]
const UPDATE_PANEL_HTML: &str = include_minifier::include_minified!("src/update.html");
#[cfg(not(feature = "ota"))]
const UPDATE_PANEL_HTML: &str = "<html><body><p>OTA updates disabled</p></body></html>";

async fn handle_request(
    request: HttpRequest<'_>,
    signals: &Rc<WmInnerSignals>,
    wifi_panel_str: &'static str,
    socket: &mut TcpSocket<'_>,
) {
    match (request.method, request.path) {
        ("GET", "/") => {
            _ = write_response(socket, "200 OK", "text/html", wifi_panel_str.as_bytes()).await;
        }
        ("GET", "/update") => {
            _ = write_response(socket, "200 OK", "text/html", UPDATE_PANEL_HTML.as_bytes()).await;
        }
        ("GET", "/list") => {
            let scan_res = signals.wifi_scan_res.try_lock();
            let resp = match scan_res {
                Ok(ref resp) => resp.as_str(),
                Err(_) => "",
            };
            _ = write_response(socket, "200 OK", "text/plain", resp.as_bytes()).await;
        }
        ("POST", "/setup") => {
            let body_vec = request.body.to_vec();
            signals.wifi_conn_info_sig.signal(body_vec);
            _ = write_response(socket, "200 OK", "text/plain", b".").await;
        }
        _ => {
            _ = write_response(socket, "404 Not Found", "text/plain", b"Not Found").await;
        }
    }
}

#[embassy_executor::task(pool_size = WEB_TASK_POOL_SIZE)]
async fn web_task(
    _id: usize,
    stack: Stack<'static>,
    signals: Rc<WmInnerSignals>,
    wifi_panel_str: &'static str,
) {
    log::info!("starting http listener...");
    let fut = async {
        let mut rx_buffer = [0; 1024];
        let mut tx_buffer = [0; 1024];
        let mut http_buffer = alloc::vec![0; HTTP_BUFFER_SIZE];

        let mut socket = TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);
        socket.set_timeout(Some(Duration::from_secs(10)));
        socket.set_nagle_enabled(false);
        loop {
            if socket.accept(80).await.is_err() {
                Timer::after(Duration::from_millis(5)).await;
                continue;
            }

            log::info!("new incoming connection");
            // read req
            let mut total_read = 0;
            loop {
                match socket.read(&mut http_buffer[total_read..]).await {
                    Ok(0) => break,
                    Ok(n) => {
                        total_read += n;
                        if http_buffer[..total_read]
                            .windows(4)
                            .any(|w| w == b"\r\n\r\n")
                        {
                            break;
                        }
                        if total_read >= HTTP_BUFFER_SIZE {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            if total_read == 0 {
                socket.close();
                continue;
            }

            // parse and handle request
            if let Some(req) = parse_http_request(&http_buffer[..total_read]) {
                if req.path.starts_with("/update") && req.method.to_uppercase() == "POST" {
                    #[cfg(feature = "ota")]
                    if handle_update_req(req, &mut socket).await.is_none() {
                        _ = write_response(
                            &mut socket,
                            "500 Internal Server Error",
                            "text/plain",
                            b"Update handler failed",
                        )
                        .await;
                    }
                } else {
                    handle_request(req, &signals, wifi_panel_str, &mut socket).await;
                }
            }

            Timer::after_millis(5).await;
            socket.close();
            Timer::after_millis(5).await;
            socket.abort();
        }
    };

    embassy_futures::select::select(fut, signals.end_signalled()).await;
}

#[cfg(feature = "ota")]
async fn handle_update_req(req: HttpRequest<'_>, socket: &mut TcpSocket<'_>) -> Option<()> {
    let query = req.path.split("?").nth(1)?;

    let mut query = query.split("&").map(|q| {
        let mut split = q.split("=");
        (
            split.next().unwrap_or_default(),
            split.next().unwrap_or_default(),
        )
    });

    let size: u32 = query.find(|(k, _)| *k == "size")?.1.trim().parse().ok()?;
    let crc: u32 = query.find(|(k, _)| *k == "crc")?.1.trim().parse().ok()?;

    log::info!("Start ota update. Size: {size} crc: {crc}");
    let headers = core::str::from_utf8(req.headers).ok()?;
    let content_length: usize = headers
        .split("\r\n")
        .map(|h| {
            let mut split = h.splitn(2, ": ");
            let k = split.next().unwrap_or_default();
            let v = split.next().unwrap_or_default();
            (k, v)
        })
        .find(|(k, _)| k.to_uppercase() == "CONTENT-LENGTH")?
        .1
        .trim()
        .parse()
        .ok()?;

    let mut ota = esp_hal_ota::Ota::new(esp_storage::FlashStorage::new(unsafe {
        esp_hal::peripherals::FLASH::steal()
    }))
    .ok()?;
    let res = ota.ota_begin(size, crc);
    if let Err(e) = res {
        log::warn!("Ota begin error: {e:?}");
        return None;
    }

    let mut ota_buffer = alloc::vec![0u8; 4096];
    ota_buffer[..req.body.len()].copy_from_slice(req.body);
    let mut buffer_pos = req.body.len();
    let mut total = 0;

    loop {
        match socket.read(&mut ota_buffer[buffer_pos..]).await {
            Ok(0) => {
                if buffer_pos > 0 {
                    total += buffer_pos;
                    log::info!("read body: {} (total: {}) - final chunk", buffer_pos, total);
                    let res = ota.ota_write_chunk(&ota_buffer[..buffer_pos]);
                    if res == Ok(true) {
                        if ota.ota_flush(true, true).is_ok() {
                            log::info!("OTA restart!");
                            if !write_response(
                                socket,
                                "200 OK",
                                "text/plain",
                                b"OTA Update Successful. Restarting...",
                            )
                            .await
                            {
                                return None;
                            }
                            Timer::after(Duration::from_millis(100)).await;
                            esp_hal::system::software_reset();
                        } else {
                            log::error!("OTA flash verify failed!");
                        }
                    }
                }
                break;
            }
            Ok(n) => {
                buffer_pos += n;

                if buffer_pos == 4096 || total + buffer_pos >= content_length {
                    total += buffer_pos;
                    log::info!("read body: {} (total: {})", buffer_pos, total);

                    let progress_msg = format!("PROGRESS:{},{}\n", total, content_length);
                    _ = socket.write_all(progress_msg.as_bytes()).await;
                    _ = socket.flush().await;

                    let res = ota.ota_write_chunk(&ota_buffer[..buffer_pos]);
                    if res == Ok(true) {
                        if ota.ota_flush(true, true).is_ok() {
                            log::info!("OTA restart!");
                            let final_msg = "DONE:OTA Update Successful. Restarting...\n";
                            _ = socket.write_all(final_msg.as_bytes()).await;
                            _ = socket.flush().await;

                            Timer::after(Duration::from_millis(100)).await;
                            esp_hal::system::software_reset();
                        } else {
                            log::error!("OTA flash verify failed!");
                        }
                    }
                    buffer_pos = 0;

                    if total >= content_length {
                        break;
                    }
                }
            }
            Err(_) => break,
        }
    }

    _ = write_response(socket, "200 OK", "text/html", b"Uploaded").await;

    Some(())
}

pub async fn run_http_server(
    spawner: &Spawner,
    ap_stack: Stack<'static>,
    signals: Rc<WmInnerSignals>,
    wifi_panel_str: &'static str,
) {
    for id in 0..WEB_TASK_POOL_SIZE {
        spawner.spawn(
            web_task(id, ap_stack, signals.clone(), wifi_panel_str).expect("Web task failed"),
        );
    }
}
