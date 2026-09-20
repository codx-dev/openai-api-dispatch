default:
  just --list --list-submodules

check: check-format check-clippy check-test check-doc check-features

check-format:
  cargo fmt --all -- --check

check-clippy:
  cargo clippy --workspace --all-targets --all-features -- -D warnings

check-test:
  cargo hack test --feature-powerset

check-doc:
  RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps

check-nats model="test" url="nats://127.0.0.1:4222":
  OPENAI_API_DEFAULT_MODEL="{{ model }}" OPENAI_API_NATS_URL="{{ url }}" \
    cargo test --features nats-queue nats -- --ignored

check-jetstream model="test" url="nats://127.0.0.1:4222":
  test_namespace="openai_api_dispatch_tests_{{ uuid() }}"; \
    OPENAI_API_DEFAULT_MODEL="{{ model }}" \
    OPENAI_API_NATS_URL="{{ url }}" \
    OPENAI_API_NATS_STREAM="$test_namespace" \
    OPENAI_API_NATS_BUCKET="$test_namespace" \
    OPENAI_API_NATS_MEMORY_STORAGE=1 \
    cargo test --features jetstream-queue jetstream -- --ignored

check-openai model="test" url="http://127.0.0.1:8000/v1":
  OPENAI_API_DEFAULT_MODEL="{{ model }}" OPENAI_API_URL="{{ url }}" \
    cargo test openai -- --ignored

check-features:
  RUSTFLAGS="-D warnings" cargo check --all-targets
  RUSTFLAGS="-D warnings" cargo hack check \
    --feature-powerset \
    --no-dev-deps
  RUSTFLAGS="-D warnings" cargo hack check \
    --target thumbv7em-none-eabi \
    --feature-powerset \
    --exclude-features std,default,nats-queue,jetstream-queue \
    --no-dev-deps
