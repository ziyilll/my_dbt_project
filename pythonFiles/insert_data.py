from faker import Faker
from pyhive import hive
import random

# 创建 Faker 实例
fake = Faker()

# 连接到 Hive
try:
    print("Connecting to Hive...")
    conn = hive.Connection(host='localhost', port=10000, username='hive', auth='NOSASL')
    cursor = conn.cursor()
    print("Connected to Hive.")

    # 准备插入数据的 SQL
    insert_sql = """
    INSERT INTO traffic (session_id, user_id, video_id, view_duration, interaction_type, product_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # 生成虚假数据并插入 Hive 表
    for i in range(1000):  # 生成1000条记录
        session_id = fake.uuid4()
        user_id = fake.user_name()
        video_id = fake.uuid4()
        view_duration = random.randint(60, 3600)  # 观看时长在1分钟到1小时之间
        interaction_type = random.choice(['like', 'favorite', 'comment'])
        product_id = fake.uuid4()

        cursor.execute(insert_sql, (session_id, user_id, video_id, view_duration, interaction_type, product_id))
        print(f"Inserted record {i + 1}")

    # 关闭连接
    conn.close()
    print("Data insertion complete and connection closed.")
except Exception as e:
    print(f"Failed to connect to Hive or insert data: {e}")
