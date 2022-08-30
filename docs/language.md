## Design Goals

- Efficient and scalable
- Supports spatial tests
- Supports homebrew (researcher-written) tests (Python?)
- Maintainable going forward

# Go

## Efficiency (0/5)

If we link titanlib to support spatial tests, this becomes a pretty grim picture. The FFI and SWIG wrappers add significant overhead, and the fact that CGo calls consume an entire system thread makes goroutines unviable.

## Spatial test support (0/5)

Can be achieved by linking titanlib via SWIG, but this compromises the other design goals. It is unfeasable to write spatial tests in Go, as it does not have suitable geoscience libraries.

## Homebrew test support (0/5)

No native CPython bindings. We would need to either exec Python as an external process, or use CPython bindings via CGo (2 layers of FFI).

## Maintainability (0/5)

If we link titanlib to support spatial tests, we have to deal with linking in our build and deployment systems, manual memory management, and would lose the guarantees of native concurrency primitives. Any future maintainer would need to be a competent programmer in both Go and C++, and have a good understanding of CGo and SWIG, Which seems like a big ask.

# C++

## Efficiency (5/5)

No notes.

## Spatial test support (5/5)

Can call titanlib directly.

## Homebrew test support (5/5)

Native CPython bindings.

## Maintainability (?/5)

Lack of modern tooling adds a significant maintenance burden. The language is in decline, so it's unclear how easy it will be to find competency going forward. The developer on the project has little familiarity or comfort with C++.

# Rust

## Efficiency (5/5)

No notes.

## Spatial test support (5/5)

Good geoscience libraries available.

## Homebrew test support (5/5)

Native CPython bindings.

## Maintainability (?/5)

Modern toolchain, growing language. New-ish to met, only used by one other team. The developer on the project is experienced and productive with Rust.
