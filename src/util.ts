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
