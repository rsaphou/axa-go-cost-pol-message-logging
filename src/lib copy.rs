//Working without error
mod generated;
use anyhow::{anyhow, Result};
use pdk::hl::*;
use pdk::logger;
use serde_json::json;
extern crate serde_json;
use serde_json::{Value};
use crate::generated::config::Config;
use pdk::metadata::Metadata;
use chrono::{Local,DateTime};
use pdk::jwt::JWTClaimsParser;
use pdk::jwt::TokenProvider;
use regex::Regex;
use url::Url;

async fn request_filter(request_state: RequestState, _config: &Config, metadata: &Metadata, stream: StreamProperties) -> Flow<String> {
    
    let headers_state = request_state.into_headers_state().await;
    let mut client_id: Option<String> = Default::default();
    let mut scope: Option<String> = Default::default();
    let mut audience: Option<String> = Default::default();
    let mut issuer: Option<String> = Default::default();
    
    let auth_header = headers_state.handler().header("authorization").unwrap_or_default();
    if !auth_header.is_empty() {
        let token = TokenProvider::bearer(headers_state.handler());
        if token.is_err() {
            
        } else {
            
            let parsed_claims = JWTClaimsParser::parse(token.unwrap());
            
            if parsed_claims.is_err() {
                
            } else {
                let claims = parsed_claims.unwrap();
                
                client_id = claims.get_claim("client_id");
                scope = claims.get_claim("scope");
                audience = claims.get_claim("aud");
                issuer = claims.get_claim("iss");
            }
        }
    }else {
            
        }
    
    let start_time: DateTime<Local> = Local::now();
    let formatted_start_time = start_time.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
    let path = headers_state.path();
    let url = headers_state.authority();
    let method = headers_state.method();
    let proto = headers_state.handler().header("x-forwarded-proto").unwrap_or_default();
    let modified_url = format!("{}://{}", proto, url);
    let request_content_type = headers_state.handler().header("content-type").unwrap_or_default(); 
    let user_agent = headers_state.handler().header("user-agent").unwrap_or_default();
    let mut query_string = String::new();
    if let Some(index) = path.find('?'){
        query_string = path[index + 1..].to_string();
    }
    let mut version: String = String::new();
    let mut label: String = String::new();
    let label_version = metadata.api_metadata.version.clone().unwrap_or_default();
    let re = Regex::new(r"^[a-zA-Z]+-[a-zA-Z]+-[a-zA-Z]+-v\d").unwrap();
    if re.is_match(&label_version) && !label_version.is_empty() {
        let mut parts = label_version.split('-').collect::<Vec<&str>>();
        version = parts.pop().unwrap_or("").to_string();
        label = format!("{}-{}-{}", parts[0], parts[1], parts[2]);
    }else{
       
    }
    
	let tls_version = String::from_utf8(stream.read_property(&["connection", "tls_version"]).unwrap_or_default()).unwrap_or_default();
    let trace_id = String::from_utf8(stream.read_property(&["request", "id"]).unwrap_or_default()).unwrap_or_default();
	
    let url = Url::parse(&modified_url).unwrap();
   //let port = url.port_or_known_default().unwrap();

    let request_data_value = json!({ 
        "http.request.referrer": modified_url,
        //"url.domain": url,
        //"url.path": path,
        //"http.request.method": method,
        //"http.response.mime_type": request_content_type,
        "client.user.id": client_id,
        "url.query": query_string,
        //"api.id": metadata.api_metadata.id.clone().unwrap(),
        //"api.name": metadata.api_metadata.name.clone().unwrap(),
        //"api.label": metadata.api_metadata.version.clone().unwrap(),
        "api.label": label,
        //"api.version": version,
        //"user_agent.original": user_agent,
        "event.start": formatted_start_time,
        //"http.request.body.bytes": body_bytes,
        "client.user.jwt.scope": scope,
        "client.user.jwt.audience": audience,
        "client.user.jwt.issuer": issuer,
		"tls.version": tls_version,
        "traceId": trace_id
        //"port":port
});
    Flow::Continue(request_data_value.to_string())
    
}


async fn response_filter(response_state: ResponseState, _config: &Config, request_data: RequestData<String> ) {
    let end_time: DateTime<Local> = Local::now();
    let formatted_end_time = end_time.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
    let headers_state = response_state.into_headers_state().await;
    let response_content_type = headers_state.handler().header("content-type").unwrap_or_default();
    let response_status = headers_state.status_code();
   // let rate_limit = headers_state.handler().header("x-ratelimit-remaining").unwrap_or_default();
    let response_time = headers_state.handler().header("x-envoy-upstream-service-time").unwrap_or_default();
    let mut level_val;
    let mut response_error = String::new();
    let body_bytes;
    let mut body_content = String::new();
    if !response_status.to_string().starts_with("20") {

        response_error = response_status.to_string();
        level_val = "ERROR";
        let response_body_state = headers_state.into_body_state().await;
        let get_body_bytes = response_body_state.handler().body().len();
        body_content = String::from_utf8_lossy(&response_body_state.handler().body()).to_string();
        body_bytes = get_body_bytes.to_string();
    }else{
        level_val = "INFO";
        let response_body_state = headers_state.into_body_state().await;
        let get_body_bytes = response_body_state.handler().body().len();
        body_bytes = get_body_bytes.to_string();
    }

    if let RequestData::Continue(request_data_value) = request_data {
      let log_data;
      if !response_status.to_string().starts_with("20"){
        level_val = "ERROR";
        log_data = json!({
            "http.response.mime_type": response_content_type,
            "http.response.status_code": response_status,
            //"error.code": response_error,
            "http.response.body.bytes": body_bytes,
            //"level": level_val,
            "http.response.body.content":body_content
         });
      }else{
        level_val = "INFO";
        log_data = json!({
            "http.response.mime_type": response_content_type,
            "http.response.status_code": response_status,
            //"error.code": response_error,
            "event.end": formatted_end_time,
            "event.duration": response_time,
            "http.response.body.bytes": body_bytes,
            //"level": level_val
         });
      }
      

    let request_data_string: Value = serde_json::from_str(&request_data_value).unwrap();
    let mut combined_json = request_data_string.as_object().unwrap().clone();

    for (key, value) in log_data.as_object().unwrap().iter() {
        combined_json.insert(key.clone(), value.clone());
    }
    let combined_json_value: Value = Value::Object(combined_json);

    if level_val == "INFO"
    { logger::info!("[accessLog] {}",combined_json_value); }
    else
    { logger::error!("[accessLog] {}",combined_json_value); }
    } else {
        return;
    };
}



#[entrypoint]
async fn configure(launcher: Launcher, Configuration(bytes): Configuration, metadata: Metadata) -> Result<()> {
    let config: Config = serde_json::from_slice(&bytes).map_err(|err| {
        anyhow!(
            "Failed to parse configuration '{}'. Cause: {}",
            String::from_utf8_lossy(&bytes),
            err
        )
    })?;
    let filter = on_request(|rs, stream| request_filter(rs, &config, &metadata, stream))
    .on_response(|res, request_data| {
        response_filter(res, &config, request_data) 
    });
    launcher.launch(filter).await?;
    Ok(())
}