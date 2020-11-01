import pandas as pd
import json, os
from elasticsearch import Elasticsearch
from tkinter import *
from tkinter import filedialog
from threading import Thread


def insert_to_es():
    index_name = myindex.get()
    
    addr = {"host": host.get(), "port": port.get()}
    es = Elasticsearch(hosts=[addr])

    df = pd.read_csv(csv_addr, low_memory=False)

    num = int(limit.get())

    i = 1
    for item in os.listdir(directory):
        if item.endswith(".json"):
            file_name = directory + '/' + item
            f = open(file_name)
            content = f.read()
            data_to_index = json.loads(content)
            paper_id = data_to_index['paper_id']
            pub_date = df.loc[df['sha'] == paper_id]['publish_time'].values

            if (len(pub_date) == 0):
                pub_date_to_save = '2000-01-01'
            else:
                pub_date_to_save = pub_date[0]

            data_to_index['publish_date'] = pub_date_to_save

            es.index(index=index_name, ignore=400, body=data_to_index)

            percentage=str(int(i/num*100))+'%'
            save_succes.config(text=percentage)
            i = i + 1
            if (i > num):
                break

    save_succes.config(text='Inserted to index successfully!')


def save_to_index():
    save_succes.config(text='Please wait...')
    delete_succes.config(text='')
    control_thread = Thread(target=insert_to_es, daemon=True)
    control_thread.start()


def delete_index():
    index_name = myindex.get()
    addr={"host": host.get(), "port": port.get()}
    es = Elasticsearch(hosts=[addr])
    es.indices.delete(index=index_name, ignore=[400, 404])
    delete_succes.config(text='Deleted index successfully!')
    save_succes.config(text='')


def search_part2():
    index_name = myindex.get()
    addr = {"host": host.get(), "port": port.get()}
    es = Elasticsearch(hosts=[addr])

    my_title = query_part2_title.get()
    my_abstract = query_part2_abstract.get()
    my_pub_date = query_part2_date.get()
    my_size=int(query_part2_num.get())


    result_query = es.search(index=index_name,size=my_size,
                                body={"query":{
                                        "bool": {
                                          "should": [
                                            {
                                              "match": {
                                                "metadata.title": {
                                                  "query": my_title
                                                }
                                              }
                                            },
                                            {
                                              "match": {
                                                "abstract.text": {
                                                  "query": my_abstract
                                                }
                                              }
                                            },
                                            {
                                              "range": {
                                                "publish_date": {
                                                  "gte": my_pub_date
                                                }
                                              }
                                            }
                                          ]
                                        }
                                      }})


    listbox.delete(0, END)
    listbox.insert(END, "Got %d Hits:" % result_query['hits']['total']['value'])

    i = 1
    for hit in result_query['hits']['hits']:
        listbox.insert(END, '------------------------------- %i ---------------------------------' % i
                       ,"Title: "+str(hit["_source"]["metadata"]['title'])
                       ,"Score: "+str(hit["_score"]))
        i += 1


def search_part3():
    index_name = myindex.get()
    addr = {"host": host.get(), "port": port.get()}
    es = Elasticsearch(hosts=[addr])

    my_title = query_part3_title.get()
    my_abstract = query_part3_abstract.get()
    my_pub_date = query_part3_date.get()
    title_weight=int(query_part3_title_weight.get())
    abstract_weight=int(query_part3_abstract_weight.get())
    pub_date_weight=int(query_part3_date_weight.get())


    result_query = es.search(index=index_name,
                             body={"query": {
                                 "bool": {
                                     "should": [
                                         {
                                             "match": {
                                                 "metadata.title": {
                                                     "query": my_title,
                                                     "boost": title_weight
                                                 }
                                             }
                                         },
                                         {
                                             "match": {
                                                 "abstract.text": {
                                                     "query": my_abstract,
                                                     "boost": abstract_weight
                                                 }
                                             }
                                         },
                                         {
                                             "range": {
                                                 "publish_date": {
                                                     "gte": my_pub_date,
                                                     "boost": pub_date_weight
                                                 }
                                             }
                                         }
                                     ]
                                 }
                             }})


    listbox2.delete(0, END)
    listbox2.insert(END, "Got %d Hits:" % result_query['hits']['total']['value'])

    i = 1
    for hit in result_query['hits']['hits']:
        listbox2.insert(END, '------------------------------- %i ---------------------------------' % i
                       ,"Title: "+str(hit["_source"]["metadata"]['title']))
        i += 1



def FolderBrowse():
    global directory
    directory=filedialog.askdirectory(initialdir = "/", title = "Open Folder")
    dir_path.config(text=directory)
    save_succes.config(text='')
    delete_succes.config(text='')

def FileBrowse():
    global csv_addr
    csv_addr=filedialog.askopenfilename(initialdir = "/", title = "Open File", filetypes = (("csv files","*.csv"), ("all files","*.*")))
    csv_path.config(text=csv_addr)
    save_succes.config(text='')
    delete_succes.config(text='')



window = Tk()

window.title("AdvancedIR-Project")

window.configure(height='550px', width='900px')


Label(window,text="index:").place(x=300,y=20)
myindex = Entry(window)
myindex.place(x=340,y=20)


Label(window,text="host:").place(x=490,y=20)
host = Entry(window)
host.place(x=525,y=20)


Label(window,text="port:").place(x=690,y=20)
port = Entry(window)
port.place(x=725,y=20)


Label(window,text="--------------------------------------------------------------------------------------------------------------- Part 1 ---------------------------------------------------------------------------------------------------------------------").place(x=10,y=60)


Button(text='Choose JSON files Directory', command=FolderBrowse).place(x=10,y=90)

dir_path = Label(window, text='',bg='white',bd=4)
dir_path.place(x=180,y=90)


Button(text='Choose CSV file', command=FileBrowse).place(x=650,y=90)

csv_path = Label(window, text='',bg='white',bd=4)
csv_path.place(x=755,y=90)


Label(window,text="Number of JSON files to index:").place(x=10,y=140)
limit = Entry(window,width=8)
limit.place(x=180,y=140)


Button(text='Save to index', command=save_to_index).place(x=450,y=140)
save_succes = Label(window, text='', fg='green', bd=4)
save_succes.place(x=535,y=140)


Button(text='Delete index', command=delete_index).place(x=820,y=140)
delete_succes = Label(window, text='', fg='green', bd=4)
delete_succes.place(x=900,y=140)


Label(window,text="--------------------------------------------------------------------------------------------------------------- Part 2 ---------------------------------------------------------------------------------------------------------------------").place(x=10,y=190)


Label(window,text="Title:").place(x=10,y=230)
query_part2_title = Entry(window, width=40)
query_part2_title.place(x=50,y=230)


Label(window,text="Abstract:").place(x=350,y=230)
query_part2_abstract = Entry(window, width=65)
query_part2_abstract.place(x=410,y=230)


Label(window,text="Publish date:").place(x=870,y=230)
query_part2_date = Entry(window, width=20)
query_part2_date.place(x=950,y=230)


Label(window,text="Number of returned results:").place(x=10,y=270)
query_part2_num = Entry(window, width=10)
query_part2_num.place(x=170,y=270)


Button(text='Search', command=search_part2,width=10).place(x=545,y=265)


scrollbar = Scrollbar(window)

scrollbar.place(x=1015,y=310,height=110)

listbox = Listbox(window, yscrollcommand=scrollbar.set)

listbox.place(x=160,y=310,width=856,height=110)
scrollbar.config(command = listbox.yview)


Label(window,text="--------------------------------------------------------------------------------------------------------------- Part 3 ---------------------------------------------------------------------------------------------------------------------").place(x=10,y=440)


Label(window,text="Title:").place(x=10,y=480)
query_part3_title = Entry(window, width=40)
query_part3_title.place(x=50,y=480)


Label(window,text="Abstract:").place(x=350,y=480)
query_part3_abstract = Entry(window, width=65)
query_part3_abstract.place(x=410,y=480)


Label(window,text="Publish date:").place(x=870,y=480)
query_part3_date = Entry(window, width=20)
query_part3_date.place(x=950,y=480)


Label(window,text="Title weight:").place(x=10,y=520)
query_part3_title_weight = Entry(window, width=10)
query_part3_title_weight.place(x=85,y=520)


Label(window,text="Abstract weight:").place(x=350,y=520)
query_part3_abstract_weight = Entry(window, width=10)
query_part3_abstract_weight.place(x=445,y=520)


Label(window,text="Publish weight:").place(x=870,y=520)
query_part3_date_weight = Entry(window, width=10)
query_part3_date_weight.place(x=960,y=520)


Button(text='Search', command=search_part3,width=10).place(x=40,y=595)


scrollbar2 = Scrollbar(window)

scrollbar2.place(x=1015,y=560,height=110)

listbox2 = Listbox(window, yscrollcommand=scrollbar2.set)


listbox2.place(x=160,y=560,width=856,height=110)
scrollbar2.config(command = listbox2.yview)


#creating a loop for the main window to store the changes
window.mainloop()
