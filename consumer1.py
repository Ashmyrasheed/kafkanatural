from kafka import KafkaConsumer

import mysql.connector



boostrap_server=["localhost:9092"]

try:

    mydb=mysql.connector.connect(host='localhost',user='root',password='',database='naturalnumbers')

except mysql.connector.Error as e:

    print("db eror",e)

mycursor=mydb.cursor()  



topic="Naturalnumber"




consumer=KafkaConsumer(topic,bootstrap_servers=boostrap_server)

for i in consumer:

    print(str(i.value.decode()))

    reverse=int (i.value.decode())

    num=0

    while reverse > 0:

        reminder= reverse % 10

        num=(num * 10) + reminder

        reverse//=10

    sql="INSERT INTO `reverse`(`reverseno`) VALUES (%s)"

    data1=(num,)

    mycursor.execute(sql,data1)

    mydb.commit()

    print("even number add to db" ,num)