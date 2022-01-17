from random import randint

def arrojar_dado():
    return randint(1, 6)

def elegir_dado_mas_alto(ti):
    dados = ti.xcom_pull(task_ids=[
        'dado_A',
        'dado_B',
        'dado_C'
    ])
    dado_max = max(dados)
    ti.xcom_push(key="dado_max_value", value=dado_max)

    if (dado_max % 2 == 0):
        return 'es_par'
    return 'es_impar'
