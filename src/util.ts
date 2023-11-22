import { BadRequest, InternalServerError } from "@hasura/ndc-sdk-typescript";
import { Err, Ok, Result } from "./result";

export const unreachable = (x: never): never => { throw new Error(`Unreachable code reached! The types lied! 😭 Unexpected value: ${x}`) };

export function isArray(x: unknown): x is unknown[] {
  return Array.isArray(x);
}

export function ifNotNull<T, U>(x: T | null, fn: (x: T) => U | null): U | null {
  return x !== null
    ? fn(x)
    : null;
}

export function mapObjectValues<U>(obj: object, fn: (value: unknown, propertyName: string) => U): Record<string, U>;
export function mapObjectValues<T, U>(obj: { [k: string]: T }, fn: (value: T, propertyName: string) => U): Record<string, U>;
export function mapObjectValues<T, U>(obj: { [k: string]: T }, fn: (value: T, propertyName: string) => U): Record<string, U> {
  return Object.fromEntries(Object.entries(obj).map(([prop, val]) => [prop, fn(val, prop)]));
}

// Throws an internal server error. Useful for using after a short-circuiting ?? operator to eliminate null/undefined from the type
export function throwInternalServerError<T>(...args: ConstructorParameters<typeof InternalServerError>): NonNullable<T> {
  throw new InternalServerError(...args);
}

// Throws an bad request error. Useful for using after a short-circuiting ?? operator to eliminate null/undefined from the type
export function throwBadRequest<T>(...args: ConstructorParameters<typeof BadRequest>): NonNullable<T> {
  throw new BadRequest(...args);
}

export function findWithIndex<T>(array: T[], predicate: (value: T, index: number, obj: T[]) => boolean): Result<[T, number], undefined> {
  const index = array.findIndex(predicate);
  return index !== -1
    ? new Ok([array[index]!, index])
    : new Err(undefined);
}
