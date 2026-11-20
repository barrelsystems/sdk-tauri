/// Sends the periodic check-in request and parses its response envelope.
pub mod check_in;
/// Verifies and parses a signed `Policy` from a check-in/pre-action-check response.
pub mod policy;
/// Asks whether it's safe to proceed with a download/apply right now.
pub mod pre_action_check;
mod protocol_request;
