  
import os
import mysql.connector
import csv 
from zipfile import ZipFile
print(os.getcwd())

directory = input('Directory: ')
print('Database connectrions..')
host = input('host: ')
user = input('User: ')
password = input('password (if none press enter): ')

mydb = mysql.connector.connect(host=host, user=user, password=password)
mycursor = mydb.cursor()
#mycursor.execute('CREATE DATABASE test3')
#mycursor.fetchall
#mydb = mysql.connector.connect(host='localhost', user='root', password='', database='test3')
#mycursor = mydb.cursor()

def create_db():
    mycursor = mydb.cursor()
    mycursor.execute('CREATE DATABASE test3')
    mycursor.fetchall
    

def sql_upload(sqlFile):
    os.system(f'cmd /c "mysql -u root test3 < {sqlFile}"')

def TableNames():
    raw_Table_names = []
    mydb = mysql.connector.connect(host=host, user=user, password=password, database='test3')
    mycursor = mydb.cursor()
    mycursor.execute('SHOW TABLES')
    result = mycursor.fetchall()
    raw_Table_names.append(result)
    Table_names = []
    for i in raw_Table_names:
        for x in i:
            append = str(x).replace(',)', '')
            append = append.replace('(', '')
            append = append.replace('\'', '')
            Table_names.append(append)
    return Table_names

def get_size(table_name):
    mydb2 = mysql.connector.connect(host=host, user=user, password=password, database='test3')
    cursor = mydb2.cursor()
    cursor.execute('SELECT TABLE_NAME as \'Table\', ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS `Size (MB)` FROM information_schema.TABLES WHERE TABLE_SCHEMA = \'test3\' AND TABLE_NAME = ' + '\'' + table_name + '\'' + ' ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;') 
    result = cursor.fetchall()
    result = result[0][1]
    return str(int(result)) + 'MB'

def get_row_count(table_name):
    mydb2 = mysql.connector.connect(host=host, user=user, password=password, database='test3')
    cursor = mydb2.cursor()
    cursor.execute('SELECT COUNT(*) from `' + table_name + '`')
    result = cursor.fetchall()
    result = result[0][0]
    return result

def delete_db():
    cursor = mydb.cursor()
    cursor.execute('DROP DATABASE test3')
def fileLoop():
    with open('backupInfo.csv', 'w') as f:
        os.chdir(directory)
        writer = csv.writer(f)
        for year in os.listdir():
            writer.writerow([year])
            os.chdir(directory + '\\' + year)
            for day in os.listdir():
                writer.writerow([day])
                os.chdir(directory + '\\' + year + '\\' + day)
                for Zip in os.listdir():
                    writer.writerow([Zip])
                    Zip2 = Zip.replace('.zip', '')
                    Zip2 = Zip2.replace('.sql', '')
                    os.makedirs(directory + '\\' + year + '\\' + day + '\\' + Zip2)
                    os.chdir(directory + '\\' + year + '\\' + day + '\\' + Zip2)
                    with ZipFile(directory + '\\' + year + '\\' + day + '\\' + Zip, 'r') as zip:
                        zip.extractall()
                    create_db()
                    for File in os.listdir():
                        sql_upload(File)
                        for table in TableNames():
                            writer.writerow([table, get_size(table), get_row_count(table)])
                        os.remove(File)
                    os.chdir(directory + '\\' + year + '\\' + day)
                    os.rmdir(directory + '\\' + year + '\\' + day + '\\' + Zip2)
                    delete_db()






fileLoop()