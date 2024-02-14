import asyncio
from prefect import flow, pause_flow_run
from prefect.input import RunInput
from prefect import runtime


class PineappleInPizza(RunInput):
    name: str
    i_do_like_pineapples_in_pizza: bool


async def on_completion_hook(flow, flow_run, state):
    print(f"👍 Flow run {flow_run.id} state: {state}")


async def on_failure_hook(flow, flow_run, state):
    print(f"👎 Flow run {flow_run.id} state: {state}")


@flow(log_prints=True, on_completion=[on_completion_hook], on_failure=[on_failure_hook])
async def check_pizza_preferences():
    print(f"SYSTEM INFORMATION. Deployment: {runtime.deployment.name}. Flow run: {runtime.flow_run.name} {runtime.flow_run.flow_name} {runtime.flow_run.run_count}")
    print("Hi! Please fill out the form by clicking on the 'Resume flow' button.")
    user_likes_pizza_with_pineapples = await pause_flow_run(
        wait_for_input=PineappleInPizza.with_initial_data(
                name="Guest",
                i_do_like_pineapples_in_pizza=False
            )
    )

    print(user_likes_pizza_with_pineapples)
    name = user_likes_pizza_with_pineapples.name

    if user_likes_pizza_with_pineapples.i_do_like_pineapples_in_pizza == True:
        raise ValueError(f"Hey look! {name} do likes pineapples in pizzas!")
    else:
        print("Everything is ok.")


if __name__=="__main__":
    asyncio.run(check_pizza_preferences())
