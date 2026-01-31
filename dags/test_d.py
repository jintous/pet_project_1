from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Изменено с DummyOperator на EmptyOperator
from airflow.operators.branch import BaseBranchOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_data(**context):
    """Генерация данных и отправка в XCom"""
    data = {
        'user_id': 12345,
        'name': 'Иван Иванов',
        'email': 'ivan@example.com',
        'score': 95
    }
    
    # Отправка данных в XCom
    context['ti'].xcom_push(key='user_data', value=data)
    context['ti'].xcom_push(key='process_id', value='PROC-001')
    
    return "Данные сгенерированы"

def process_data(**context):
    """Получение данных из XCom и их обработка"""
    ti = context['ti']
    user_data = ti.xcom_pull(task_ids='generate_task', key='user_data')
    process_id = ti.xcom_pull(task_ids='generate_task', key='process_id')
    
    print(f"Получены данные: {user_data}")
    print(f"Process ID: {process_id}")
    
    # Обработка данных
    user_data['processed'] = True
    user_data['processed_at'] = datetime.now().isoformat()
    user_data['score_category'] = 'Отлично' if user_data['score'] >= 90 else 'Хорошо'
    
    # Отправка обработанных данных дальше
    ti.xcom_push(key='processed_data', value=user_data)
    
    return f"Данные обработаны для пользователя {user_data['name']}"

def validate_data(**context):
    """Валидация обработанных данных"""
    ti = context['ti']
    
    # Получение данных из второй задачи
    processed_data = ti.xcom_pull(task_ids='process_task', key='processed_data')
    
    print(f"Валидация данных: {processed_data}")
    
    # Простая валидация
    if processed_data['score'] < 0 or processed_data['score'] > 100:
        raise ValueError(f"Некорректный score: {processed_data['score']}")
    
    if not processed_data.get('processed'):
        raise ValueError("Данные не были обработаны")
    
    processed_data['validated'] = True
    ti.xcom_push(key='validated_data', value=processed_data)
    
    return "Данные валидированы успешно"

def save_results(**context):
    """Финализация - получение всех данных"""
    ti = context['ti']
    
    # Получение данных из всех предыдущих задач
    original_data = ti.xcom_pull(task_ids='generate_task', key='user_data')
    processed_data = ti.xcom_pull(task_ids='process_task', key='processed_data')
    validated_data = ti.xcom_pull(task_ids='validate_task', key='validated_data')
    
    # Также можно получить возвращаемые значения задач
    gen_result = ti.xcom_pull(task_ids='generate_task')
    process_result = ti.xcom_pull(task_ids='process_task')
    validate_result = ti.xcom_pull(task_ids='validate_task')
    
    print("=" * 50)
    print("ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
    print(f"Исходные данные: {original_data}")
    print(f"Обработанные данные: {processed_data}")
    print(f"Валидированные данные: {validated_data}")
    print(f"Результат генерации: {gen_result}")
    print(f"Результат обработки: {process_result}")
    print(f"Результат валидации: {validate_result}")
    print("=" * 50)
    
    # Сохранение финального результата
    final_result = {
        'status': 'completed',
        'data': validated_data,
        'timestamp': datetime.now().isoformat(),
        'dag_run_id': context['dag_run'].run_id
    }
    
    ti.xcom_push(key='final_result', value=final_result)
    
    return "Результаты сохранены"

# Класс для условного ветвления
class BranchOperator(BaseBranchOperator):
    def choose_branch(self, context):
        ti = context['ti']
        user_data = ti.xcom_pull(task_ids='generate_task', key='user_data')
        
        score = user_data['score']
        
        if score >= 90:
            return 'high_score_branch'
        elif score >= 70:
            return 'medium_score_branch'
        else:
            return 'low_score_branch'

def high_score_handler(**context):
    """Обработчик для высоких scores"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='generate_task', key='user_data')
    print(f"Пользователь {data['name']} имеет высокий score: {data['score']}")
    return "Высокий score обработан"

def medium_score_handler(**context):
    """Обработчик для средних scores"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='generate_task', key='user_data')
    print(f"Пользователь {data['name']} имеет средний score: {data['score']}")
    return "Средний score обработан"

# Создаем DAG
with DAG(
    'xcom_example_dag_airflow3',
    default_args=default_args,
    description='Пример DAG с XCom для Airflow 3.0.0',
    schedule='@daily',
    catchup=False,
    tags=['example', 'xcom', 'airflow3', 'pet'],
) as dag:
    
    # Начальная задача - теперь используем EmptyOperator вместо DummyOperator
    start = EmptyOperator(task_id='start')
    
    # Задача генерации данных
    generate_task = PythonOperator(
        task_id='generate_task',
        python_callable=generate_data,
    )
    
    # Задача обработки данных
    process_task = PythonOperator(
        task_id='process_task',
        python_callable=process_data,
    )
    
    # Задача валидации данных
    validate_task = PythonOperator(
        task_id='validate_task',
        python_callable=validate_data,
    )
    
    # Задача условного ветвления
    branch_task = BranchOperator(
        task_id='branch_task',
    )
    
    # Ветки для разных сценариев
    high_score_task = PythonOperator(
        task_id='high_score_branch',
        python_callable=high_score_handler,
    )
    
    medium_score_task = PythonOperator(
        task_id='medium_score_branch',
        python_callable=medium_score_handler,
    )
    
    low_score_task = EmptyOperator(task_id='low_score_branch')  # Также EmptyOperator
    
    # Финализация
    save_task = PythonOperator(
        task_id='save_results',
        python_callable=save_results,
    )
    
    # Конечная задача
    end = EmptyOperator(task_id='end')
    
    # Определение порядка выполнения задач
    start >> generate_task >> process_task >> validate_task
    
    # Параллельные ветки после branch_task
    validate_task >> branch_task
    branch_task >> [high_score_task, medium_score_task, low_score_task]
    high_score_task >> save_task
    medium_score_task >> save_task
    low_score_task >> save_task
    
    save_task >> end