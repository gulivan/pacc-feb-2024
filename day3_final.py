from prefect import flow, pause_flow_run
from prefect.input import RunInput
from prefect import runtime


@flow
def foo_1():
    return 1


@flow
def foo_2():
    return 2


@flow
def main():
    result_1 = foo_1()
    result_2 = foo_2()
    return result_1 + result_2


if __name__=="__main__":
    main()
