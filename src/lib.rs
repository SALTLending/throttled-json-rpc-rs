use anyhow::{anyhow, bail, Context, Result};
use futures::channel::oneshot;
use futures::prelude::*;
use reqwest;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReqBatcher {
    ask_request: futures::channel::mpsc::Sender<RequestCallbackPair>,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub uri: String,
    pub rps: RPS,
    pub batching: usize,
    pub concurrent: usize,
    pub client_auth: Option<ClientAuth>,
}

#[derive(Debug, Clone)]
pub struct ClientAuth {
    pub user: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RPS(RpsState);

impl RPS {
    pub fn new(value: f64) -> Result<Self> {
        if value < 0.0 {
            return Err(anyhow::anyhow!("Invalid RPS value: {} <= 0", value));
        }
        if value == 0.0 {
            return Ok(RPS(RpsState::None));
        }
        Ok(RPS(RpsState::Limit(value)))
    }
    pub fn none() -> Self {
        RPS(RpsState::None)
    }
}

impl ReqBatcher {
    pub fn new(client_options: ClientOptions) -> Self {
        let (ask_request, requests) = futures::channel::mpsc::channel(10_000);
        let chunk_size = client_options.batching.max(1);
        let concurrent_count = match client_options.concurrent {
            0 => usize::max_value(),
            x => x,
        };
        let request_batcher = ReqBatcher { ask_request };
        let client_options = Arc::new(client_options);
        tokio::spawn(async move {
            requests
                .chunks(chunk_size)
                .map(|x| (client_options.clone(), x))
                .for_each_concurrent(concurrent_count, |(client_options, xs)| async move {
                    request_json_rpc(&client_options, xs)
                        .await
                        .unwrap_or_else(|e| log::error!("Error in client: {:?}", e))
                })
                .await;
        });
        request_batcher
    }

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

#[derive(Debug, Clone, Copy)]
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
) -> Result<()> {
    use serde_json::json;
    let client = reqwest::Client::new();
    let mut builder = client.post(&client_options.uri);
    let is_not_batching = client_options.batching <= 1;
    if let Some(client_auth) = &client_options.client_auth {
        builder = builder.basic_auth(client_auth.user.clone(), client_auth.password.clone());
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
    if chunk.is_empty() {
        return Ok(());
    }
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
    let response: Value = res
        .and_then(|x| x.json())
        .await
        .with_context(|| anyhow!("Calling rpc with data {:?}", chunk))?;
    let response: Vec<_> = match response {
        Value::Array(values) => values,
        x if is_not_batching => vec![x],
        x => bail!(
            "Expecting during batch that results come back as array: {:?}",
            x
        ),
    };
    for (raw_response, (method, requests, respond)) in response.into_iter().zip(chunk.into_iter()) {
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
            bail!("Failed responding for oneshot for value: {:?}", x);
        }
    }
    Ok(())
}
