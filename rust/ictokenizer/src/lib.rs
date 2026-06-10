//! C-ABI shim over `llm-tokenizer` for the inference-cache Go server.
//!
//! Exposes the minimum needed to turn (model, prompt) into engine-aligned token
//! IDs from cgo: create a tokenizer (by local path or HF model id), apply a
//! chat template, encode text, and free the resulting buffers. It depends only
//! on `llm-tokenizer` (HF/tiktoken loaders, chat-template rendering, the custom
//! DeepSeek/Kimi encoders) — not on SMG's gateway — so the static archive stays
//! free of the gateway and any Python toolchain.
//!
//! Memory ownership: every buffer the shim returns to Go (token-id arrays, error
//! strings) is freed by the matching `ic_*_free` function below — Go never frees
//! Rust allocations directly.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

use llm_tokenizer::{
    chat_template::ChatTemplateParams, create_tokenizer, traits::Tokenizer as TokenizerTrait,
};
use serde_json::Value;

/// Opaque tokenizer handle handed back to Go.
pub struct Handle {
    tokenizer: Arc<dyn TokenizerTrait>,
}

/// Status codes returned across the C ABI. 0 == success.
pub const IC_OK: i32 = 0;
pub const IC_ERR_INVALID_ARG: i32 = 1;
pub const IC_ERR_LOAD: i32 = 2;
pub const IC_ERR_ENCODE: i32 = 3;
pub const IC_ERR_PANIC: i32 = 4;

unsafe fn set_err(err_out: *mut *mut c_char, msg: &str) {
    if err_out.is_null() {
        return;
    }
    match CString::new(msg) {
        Ok(c) => *err_out = c.into_raw(),
        Err(_) => *err_out = ptr::null_mut(),
    }
}

/// Create a tokenizer from a local path (a `tokenizer.json` or a directory) or,
/// if no such path exists, an HF model id (downloaded via hf-hub; gated models
/// need HF_TOKEN). Returns a non-null handle on success, or null with `*err_out`
/// set. Free the handle with `ic_tokenizer_free`.
///
/// # Safety
/// `model_or_path` must be a valid NUL-terminated C string. The returned handle
/// must be freed exactly once with `ic_tokenizer_free`.
#[no_mangle]
pub unsafe extern "C" fn ic_tokenizer_create(
    model_or_path: *const c_char,
    err_out: *mut *mut c_char,
) -> *mut Handle {
    // Catch panics so a panicking loader (a malformed artifact, an upstream bug)
    // cannot unwind across the cgo boundary and abort the Go process — the Go
    // side turns the null return into a fail-open NO_HINT.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if model_or_path.is_null() {
            set_err(err_out, "ic_tokenizer_create: null model_or_path");
            return ptr::null_mut();
        }
        let path = match CStr::from_ptr(model_or_path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_err(err_out, "ic_tokenizer_create: model_or_path is not valid UTF-8");
                return ptr::null_mut();
            }
        };
        match create_tokenizer(path) {
            Ok(tokenizer) => Box::into_raw(Box::new(Handle { tokenizer })),
            Err(e) => {
                set_err(err_out, &e.to_string());
                ptr::null_mut()
            }
        }
    }));
    match r {
        Ok(p) => p,
        Err(_) => {
            set_err(err_out, "ic_tokenizer_create: panic in tokenizer loader");
            ptr::null_mut()
        }
    }
}

unsafe fn emit_ids(ids: &[u32], ids_out: *mut *mut u32, len_out: *mut usize) {
    let boxed = ids.to_vec().into_boxed_slice();
    *len_out = boxed.len();
    *ids_out = Box::into_raw(boxed) as *mut u32;
}

/// Tokenize already-rendered text (no chat template applied). Writes a token-id
/// array to `*ids_out` (free with `ic_free_ids`) and its length to `*len_out`.
///
/// # Safety
/// `handle` must be a live handle from `ic_tokenizer_create`; `text` a valid
/// NUL-terminated C string; `ids_out`/`len_out` non-null.
#[no_mangle]
pub unsafe extern "C" fn ic_tokenizer_encode_text(
    handle: *mut Handle,
    text: *const c_char,
    add_special_tokens: i32,
    ids_out: *mut *mut u32,
    len_out: *mut usize,
    err_out: *mut *mut c_char,
) -> i32 {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if handle.is_null() || text.is_null() || ids_out.is_null() || len_out.is_null() {
            set_err(err_out, "ic_tokenizer_encode_text: null argument");
            return IC_ERR_INVALID_ARG;
        }
        let text = match CStr::from_ptr(text).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_err(err_out, "ic_tokenizer_encode_text: text is not valid UTF-8");
                return IC_ERR_INVALID_ARG;
            }
        };
        let tokenizer = &(*handle).tokenizer;
        match tokenizer.encode(text, add_special_tokens != 0) {
            Ok(encoding) => {
                emit_ids(encoding.token_ids(), ids_out, len_out);
                IC_OK
            }
            Err(e) => {
                set_err(err_out, &e.to_string());
                IC_ERR_ENCODE
            }
        }
    }));
    match r {
        Ok(rc) => rc,
        Err(_) => {
            set_err(err_out, "ic_tokenizer_encode_text: panic in tokenizer");
            IC_ERR_PANIC
        }
    }
}

/// Apply the model's chat template to a JSON array of messages
/// (`[{"role":...,"content":...}, ...]`) then tokenize the rendered text. This
/// is the path that reproduces the tokens a chat engine caches.
///
/// # Safety
/// `handle` must be live; `messages_json` a valid NUL-terminated C string;
/// `ids_out`/`len_out` non-null.
#[no_mangle]
pub unsafe extern "C" fn ic_tokenizer_encode_chat(
    handle: *mut Handle,
    messages_json: *const c_char,
    add_generation_prompt: i32,
    ids_out: *mut *mut u32,
    len_out: *mut usize,
    err_out: *mut *mut c_char,
) -> i32 {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if handle.is_null() || messages_json.is_null() || ids_out.is_null() || len_out.is_null() {
            set_err(err_out, "ic_tokenizer_encode_chat: null argument");
            return IC_ERR_INVALID_ARG;
        }
        let messages_str = match CStr::from_ptr(messages_json).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_err(err_out, "ic_tokenizer_encode_chat: messages_json is not valid UTF-8");
                return IC_ERR_INVALID_ARG;
            }
        };
        let messages: Vec<Value> = match serde_json::from_str(messages_str) {
            Ok(m) => m,
            Err(e) => {
                set_err(err_out, &format!("ic_tokenizer_encode_chat: invalid messages JSON: {e}"));
                return IC_ERR_INVALID_ARG;
            }
        };
        let empty: [Value; 0] = [];
        let params = ChatTemplateParams {
            add_generation_prompt: add_generation_prompt != 0,
            tools: Some(&empty),
            documents: Some(&empty),
            ..Default::default()
        };
        let tokenizer = &(*handle).tokenizer;
        let rendered = match tokenizer.apply_chat_template(&messages, params) {
            Ok(s) => s,
            Err(e) => {
                set_err(err_out, &format!("apply_chat_template: {e}"));
                return IC_ERR_ENCODE;
            }
        };
        // The chat template injects special tokens, so don't add them again.
        match tokenizer.encode(&rendered, false) {
            Ok(encoding) => {
                emit_ids(encoding.token_ids(), ids_out, len_out);
                IC_OK
            }
            Err(e) => {
                set_err(err_out, &e.to_string());
                IC_ERR_ENCODE
            }
        }
    }));
    match r {
        Ok(rc) => rc,
        Err(_) => {
            set_err(err_out, "ic_tokenizer_encode_chat: panic in tokenizer");
            IC_ERR_PANIC
        }
    }
}

/// Free a token-id array returned by an encode call.
///
/// # Safety
/// `ids`/`len` must be exactly what an encode call returned, freed once.
#[no_mangle]
pub unsafe extern "C" fn ic_free_ids(ids: *mut u32, len: usize) {
    if ids.is_null() {
        return;
    }
    drop(Box::from_raw(ptr::slice_from_raw_parts_mut(ids, len)));
}

/// Free an error string returned via an `err_out` parameter.
///
/// # Safety
/// `s` must be a string this library returned, freed once.
#[no_mangle]
pub unsafe extern "C" fn ic_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    drop(CString::from_raw(s));
}

/// Free a tokenizer handle.
///
/// # Safety
/// `handle` must be a handle from `ic_tokenizer_create`, freed once.
#[no_mangle]
pub unsafe extern "C" fn ic_tokenizer_free(handle: *mut Handle) {
    if handle.is_null() {
        return;
    }
    drop(Box::from_raw(handle));
}
