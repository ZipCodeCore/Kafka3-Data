from kafka import KafkaConsumer, TopicPartition
from json import loads
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Column, Sequence


mysqlkey = os.environ('mysql_key')
Base = declarative_base()
get_session = sessionmaker()
# cust_bal_seq = Sequence('cust_bal_seq')


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

    def getSelf(self):
        return '['+str(self.id) + self.custid + str(self.type) + str(self.date) + self.date + str(self.amt)+']'



class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.engine = create_engine("mysql+pymysql://root:" + mysqlkey + "@localhost:3306/zipbank")
        # df = pd.read_csv(data_storage_path + 'nyc_park_data2.csv', delimiter=',')
        # df.to_sql(name='transaction', con=conn, schema='zipbank', if_exists='replace')


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

    
if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()