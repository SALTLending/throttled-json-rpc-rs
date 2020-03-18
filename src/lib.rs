#![deny(missing_docs, missing_debug_implementations, missing_doc_code_examples)]

//! Request batcher is a request client for anything that is json rpc [1].
//! There are quite a few limiters on this client, like the RPS, max batching, and even how many
//! allowed concurrently

use anyhow::{anyhow, Context, Result};
use futures::channel::oneshot;
use futures::prelude::*;
use reqwest;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

/// The Request Batcher is the system that will sit on a
/// json rpc client, that will limit how many at once can talk to the rpc. Granted
/// it can't know about what other clients are around, so make sure to have as few as possible of these
/// running about.
#[derive(Debug, Clone)]
pub struct ReqBatcher {
    ask_request: futures::channel::mpsc::Sender<RequestCallbackPair>,
}

/// Configurations for getting a rpc client up and running.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// This is the full path, web path like https://test:123
    pub uri: String,
    /// This is the limits on how many requests per second are allowed
    pub rps: RPS,
    /// This is how many requests to batch together to the json rpc,
    /// there are some that don't allow any batching, so the limit would be 1
    pub batching: usize,
    /// This is wondering how many connections could be opened at once
    pub concurrent: usize,
    /// Auth for RPC
    pub client_auth: Option<ClientAuth>,
}

/// Most RPC Clients have a password and username because there are methods
/// that are supposed to be restricted. For example in the bitcoin rpc's,
/// the restricted methods are those relating to the hot wallet that the system
/// uses. There are other reasons to have a password system, and that is just
/// to limit who can use the service.
#[derive(Debug, Clone)]
pub struct ClientAuth {
    /// User names
    pub user: String,
    /// Passwords, could be nothing, since there are some services that may not be protected
    pub password: Option<String>,
}

/// We want the enum behind a new type because we need to ensure that there
/// isn't a limit with 0.0 for the value. And since we are using a float we can't
/// ensure that the value is postive.
#[derive(Debug, Clone, PartialEq)]
pub struct RPS(RpsState);

impl RPS {
    /// Create a RPS with a limit being a value > 0, and as a float.
    ///
    /// ```
    /// # use throttled_json_rpc::*;
    /// assert_eq!("RPS(Limit(1.4))", &format!("{:?}", RPS::new(1.4).unwrap()),);
    /// assert_eq!("RPS(None)", &format!("{:?}", RPS::new(0.0).unwrap()),);
    /// RPS::new(-1.0).expect_err("Should error for negative");
    /// assert_eq!(RPS::none(), RPS::new(0.0).unwrap());
    /// ```
    pub fn new(value: f64) -> Result<Self> {
        if value < 0.0 {
            return Err(anyhow::anyhow!("Invalid RPS value: {} <= 0", value));
        }
        if value == 0.0 {
            return Ok(RPS(RpsState::None));
        }
        Ok(RPS(RpsState::Limit(value)))
    }
    /// Create a rps that indicates there are no limits based on requests per second
    /// ```
    /// # use throttled_json_rpc::*;
    /// assert_eq!("RPS(None)", &format!("{:?}", RPS::none()));
    /// ```
    pub fn none() -> Self {
        RPS(RpsState::None)
    }
}

impl ReqBatcher {
    /// Creating a new request batcher, one needs to send down the options for the request batcher
    pub fn new(client_options: ClientOptions) -> Self {
        let (ask_request, requests) = futures::channel::mpsc::channel(10_000);
        let chunk_size = client_options.batching.max(1);
        let concurrent_count = match client_options.concurrent {
            0 => usize::max_value(),
            x => x,
        };
        let request_batcher = ReqBatcher { ask_request };
        let client_options = Arc::new(client_options);
        let client = reqwest::Client::new();
        tokio::spawn(async move {
            requests
                .chunks(chunk_size)
                .map(|x| (client_options.clone(), client.clone(), x))
                .for_each_concurrent(
                    concurrent_count,
                    |(client_options, client, xs)| async move {
                        request_json_rpc(&client_options, xs, client).await
                    },
                )
                .await;
        });
        request_batcher
    }

    /// The point of the request batcher is to make a request. So in json rpc there are
    /// methods and the params for the rpc client. See [1]
    /// 1: https://en.wikipedia.org/wiki/JSON-RPC
    pub async fn request<'a, T>(&mut self, method: String, params: Vec<Value>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let (sender, receiver) = oneshot::channel();
        self.ask_request.try_send((method, params, sender))?;
        let received_value = receiver.await??;
        let value = serde_json::from_value(received_value.clone())
            .map_err(|_| anyhow!("Could not convert {:?} into type", received_value))?;
        Ok(value)
    }
}

type RequestCallbackPair = (String, Vec<Value>, oneshot::Sender<Result<Value>>);

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum JsonResponse {
    Result { result: Value },
    Error { error: Value },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RpsState {
    None,
    Limit(f64),
}

/**
    There are times that we want to clean the trailing nulls, because then it works better for some implementations
    of Nodes where it figures out the optionals by the count of the params via the json-rpc.
*/
fn values_cleanse(values: Vec<Value>) -> Vec<Value> {
    use Value::Null;
    values
        .into_iter()
        .rev()
        .skip_while(|some_value| match some_value {
            Null => true,
            _ => false,
        })
        .collect::<Vec<Value>>()
        .into_iter()
        .rev()
        .collect::<Vec<Value>>()
}

async fn request_json_rpc(
    client_options: &ClientOptions,
    chunk: Vec<RequestCallbackPair>,
    client: Client,
) {
    use serde_json::json;
    let mut builder = client.post(&client_options.uri);
    let is_not_batching = client_options.batching <= 1;
    if let Some(client_auth) = &client_options.client_auth {
        builder = builder.basic_auth(client_auth.user.clone(), client_auth.password.clone());
    }
    if chunk.is_empty() {
        return ();
    }
    let data: Vec<Value> = chunk
        .iter()
        .enumerate()
        .map(|(i, (method, params, ..))| {
            json!({
                "method": method,
                "params": values_cleanse(params.clone()),
                "id": i.to_string(),
            })
        })
        .collect();
    builder = if data.len() == 1 && is_not_batching {
        builder.json(&data[0])
    } else {
        builder.json(&data)
    };
    let res = builder.send();
    if let RPS(RpsState::Limit(rps)) = client_options.rps {
        let wait = if rps > 1.0 {
            std::time::Duration::from_secs(1) / rps as u32
        } else {
            std::time::Duration::from_secs(1) * (1.0 / rps) as u32
        };
        tokio::time::delay_for(wait).await;
    }
    let response: Result<Value> = res.and_then(|x| x.json()).await.with_context(|| {
        anyhow!(
            "Calling rpc with data {}",
            serde_json::to_string(&data)
                .unwrap_or_else(|_| format!("Could not decode ({:?})", data))
        )
    });

    let response: Result<Vec<_>> = match response {
        Err(e) => Err(e),
        Ok(Value::Array(values)) => {
            if values.len() == chunk.len() {
                Ok(values)
            } else {
                Err(anyhow!("Response length {} does not match callback length {} for vallback values of {:?}", values.len(), chunk.len(), values))
            }
        }
        Ok(x) if is_not_batching => Ok(vec![x]),
        Ok(x) => Err(anyhow!(
            "Expecting during batch that results come back as array: {:?}",
            x
        )),
    };
    match response {
        Ok(response) => {
            for (raw_response, (method, requests, respond)) in
                response.into_iter().zip(chunk.into_iter())
            {
                use serde_json::from_value;
                let response = match from_value(raw_response) {
                    Ok(JsonResponse::Result { result: x }) => Ok(x),
                    Ok(JsonResponse::Error { error }) => Err(anyhow!(
                        "Error response from json rpc {:?} given {:?}",
                        error,
                        (method, requests)
                    )),
                    x => Err(anyhow!(
                        "Couldn't parse json rpc response of {:?} given {:?}",
                        x,
                        (method, requests)
                    )),
                };
                if let Err(x) = respond.send(response) {
                    log::error!("Failed responding for oneshot for value: {:?}", x);
                }
            }
        }
        Err(e) => {
            for (_method, _requests, respond) in chunk.into_iter() {
                let error = Err(anyhow!("{:?}", e));
                if let Err(x) = respond.send(error) {
                    log::error!(
                        "Failed responding with error for oneshot for value: {:?}",
                        x
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{server_address, Matcher};
    use tokio_test::block_on;

    #[test]
    fn test_limited_requests_rps() {
        let mock = mockito::mock("POST", Matcher::Any)
            .with_body(r#"[{"result": "test"}]"#)
            .expect(2)
            .create();
        block_on(async {
            let mut delay = tokio::time::delay_for(tokio::time::Duration::from_millis(1100)).fuse();
            let requester = ReqBatcher::new(ClientOptions {
                uri: format!("http://{}", server_address()),
                batching: 1,
                client_auth: None,
                concurrent: 1,
                rps: RPS::new(2.0).unwrap(),
            });
            let requests: Vec<_> = (0..10)
                .map(|_i| requester.clone())
                .map(|mut requester| async move {
                    requester
                        .request::<String>("test".to_string(), vec![])
                        .await
                        .expect("testing request");
                })
                .collect();
            let mut requests = futures::future::join_all(requests).fuse();

            futures::select! {
                _ = delay => {
                }
                requests = requests => {
                    panic!("Not expecting that all the requests finnish, because it should be throttled");
                }
            }
        });
        mock.assert()
    }
    #[test]
    fn test_limited_requests_rps_concurrent() {
        let mock = mockito::mock("POST", Matcher::Any)
            .with_body(r#"[{"result": "test"}]"#)
            .expect(8)
            .create();
        block_on(async {
            let mut delay = tokio::time::delay_for(tokio::time::Duration::from_millis(2100)).fuse();

            let requester = ReqBatcher::new(ClientOptions {
                uri: format!("http://{}", server_address()),
                batching: 1,
                client_auth: None,
                concurrent: 2,
                rps: RPS::new(2.0).unwrap(),
            });
            let requests: Vec<_> = (0..10)
                .map(|_i| requester.clone())
                .map(|mut requester| async move {
                    requester
                        .request::<String>("test".to_string(), vec![])
                        .await
                        .expect("testing request");
                })
                .collect();
            let mut requests = futures::future::join_all(requests).fuse();

            futures::select! {
                _ = delay => {
                }
                requests = requests => {
                    panic!("Not expecting that all the requests finnish, because it should be throttled");
                }
            }
        });
        mock.assert();
    }

    #[test]
    #[should_panic(
        expected = r#"testing request: Response length 1 does not match callback length 2 for vallback values of [Object({"result": String("test")})]"#
    )]
    fn test_limited_requests_rps_batched_failure() {
        let mock = mockito::mock("POST", Matcher::Any)
            .with_body(r#"[{"result": "test"}]"#)
            .expect(2)
            .create();
        block_on(async {
            let mut delay = tokio::time::delay_for(tokio::time::Duration::from_millis(1100)).fuse();

            let requester = ReqBatcher::new(ClientOptions {
                uri: format!("http://{}", server_address()),
                batching: 2,
                client_auth: None,
                concurrent: 1,
                rps: RPS::new(2.0).unwrap(),
            });
            let requests: Vec<_> = (0..10)
                .map(|_i| requester.clone())
                .map(|mut requester| async move {
                    requester
                        .request::<String>("test".to_string(), vec![])
                        .await
                        .expect("testing request");
                })
                .collect();
            let mut requests = futures::future::join_all(requests).fuse();

            futures::select! {
                _ = delay => {
                }
                requests = requests => {
                    panic!("Not expecting that all the requests finnish, because it should be throttled");
                }
            }
        });
        mock.assert()
    }

    #[test]
    fn test_limited_requests_rps_batched() {
        let mock2 = mockito::mock("POST", Matcher::Any)
        .match_body(r#"[{"id":"0","method":"test","params":[]},{"id":"1","method":"test","params":[]}]"#)
            .with_body(r#"[{"result": "test"},{"result": "test2"}]"#)
            .expect(2)
            .create();

        block_on(async {
            let mut delay = tokio::time::delay_for(tokio::time::Duration::from_millis(1100)).fuse();
            let requester = ReqBatcher::new(ClientOptions {
                uri: format!("http://{}", server_address()),
                batching: 2,
                client_auth: None,
                concurrent: 1,
                rps: RPS::new(2.0).unwrap(),
            });
            let requests: Vec<_> = (0..10)
                .map(|_i| requester.clone())
                .map(|mut requester| async move {
                    requester
                        .request::<String>("test".to_string(), vec![])
                        .await
                        .expect("testing request")
                })
                .collect();
            let mut requests = futures::future::join_all(requests).fuse();

            futures::select! {
                _ = delay => {
                }
                requests = requests => {
                    panic!("Not expecting that all the requests finnish, because it should be throttled: {:?}", requests);
                }
            }
        });
        mock2.assert();
    }
}
