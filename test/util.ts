import { Err, Ok, Result } from "../src/result";
import "jest-extended"

export function expectOk<T,TErr>(result: Result<T, TErr>): T {
  expect(result).toSatisfy(function isOkResult(r) { return r instanceof Ok });
  return (result as Ok<T,TErr>).data;
}

export function expectErr<T,TErr>(result: Result<T, TErr>): TErr {
  expect(result).toSatisfy(function isErrResult(r) { return r instanceof Err });
  return (result as Err<T,TErr>).error;
}
