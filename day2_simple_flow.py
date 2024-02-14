from prefect import flow

@flow(log_prints=True)
def buy(thing: str, price: float):
    print(f"I just bought a {thing} for {price}$!")

if __name__ == "__main__":
    buy.from_source(
        source="https://github.com/gulivan/pacc-feb-2024.git",
        entrypoint="day2_simple_flow.py:buy"
    ).deploy(
        name="code-in-the-image",
        work_pool_name="docker-pool",
        image="prefect_certification:1.0",
        push=False,
    )
