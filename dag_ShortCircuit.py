import pendulum
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule


with DAG(
    dag_id='A_dag_ShortCircuit',
    start_date=pendulum.datetime(2022, 6, 27),
    catchup=False
    ) as dag:
    
    # [START howto_operator_short_circuit]
    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: False,
    )

    ds_true = [EmptyOperator(task_id='true_' + str(i)) for i in [1, 2]]
    ds_false = [EmptyOperator(task_id='false_' + str(i)) for i in [1, 2]]

    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)
    # [END howto_operator_short_circuit]

    # [START howto_operator_short_circuit_trigger_rules]
    [task_1, task_2, task_3, task_4, task_5, task_6] = [
        EmptyOperator(task_id=f"task_{i}") for i in range(1, 7)
    ]

    task_7 = EmptyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit", ignore_downstream_trigger_rules=False, python_callable=lambda: False
    )

    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)