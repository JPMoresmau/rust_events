use rust_transit::*;
use rust_transit::harness::*;


/// Test we can do a round trip via binary payload
#[test]
fn test_payload() -> Result<(),EventError>{
    let ge=GenericEvent::new("tenant1",StringEvent{message:"hello".to_owned()});
    let payload=ge.payload()?;
    let ge2=GenericEvent::from_payload(&payload)?;
    assert_eq!(ge,ge2);
    Ok(())
}