namespace java com.continuuity.common.thrift.stubs

service AddingService {
  i64 add(1: i32 num1, 2: i32 num2),
}

service SubtractingService {
  i64 sub(1: i32 num1, 2: i32 num2),
}

service MultiplyingService {
  i64 multiply(1: i32 num1, 2: i32 num2),
}