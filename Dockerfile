# syntax=docker/dockerfile:1

FROM rust:1.98-bookworm AS build
WORKDIR /src

# Dependencies first, in their own layer: a code change then rebuilds only
# the gateway itself.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --locked \
    && rm -r src

COPY src ./src
# The placeholder build left newer artifacts than the copied sources.
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# glibc, libgcc and CA certificates (for TLS to api.typesafe.ai), nothing else.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/systemone-gateway /usr/local/bin/systemone-gateway
# Mount the configuration here, and pass the TypeSafe key as TYPESAFE_API_KEY.
ENV SYSTEMONE_GATEWAY_CONFIG=/etc/systemone-gateway/gateway.toml
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/systemone-gateway"]
CMD ["serve"]
