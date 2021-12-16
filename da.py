import os
from json import dumps
import logging
from neo4j import GraphDatabase, basic_auth


from py2neo import Graph,Node,Relationship
from flask import Flask,render_template,request,g,Response
app = Flask(__name__)
url="neo4j+s://e3ec660d.databases.neo4j.io"
pwd="Bot6OCamg2jVh7kHzjulC-6zIWLjKTK6SyAcuUj40zw"
graph=Graph(url,auth=('neo4j',pwd))
query="match(v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:ACCOUNT]-(a:Account) return distinct(v.name) as ver, a.name as acc"
query2="match(a:Account) return distinct(a.name) as name"
query3="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
(b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
(ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
(oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
where da.name='Rejected' and r.name='TRUE'\
return v.name as v,oi.name as oi,on.name as on,o.name as o,b.name as b,d.name as d,s.name as s,ot.name as ot,a.name as a,r.name as r,da.name as da"
query4="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
(b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
(ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
(oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
where da.name='Approved' and r.name='TRUE' and s.name='Open'\
return v.name as v,oi.name as oi,on.name as on,o.name as o,b.name as b,d.name as d,s.name as s,ot.name as ot,a.name as a,r.name as r,da.name as da"


url = os.getenv("NEO4J_URI", "neo4j+s://e3ec660d.databases.neo4j.io")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "Bot6OCamg2jVh7kHzjulC-6zIWLjKTK6SyAcuUj40zw")
neo4jVersion = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "neo4j")



driver = GraphDatabase.driver(url, auth=basic_auth(username, password))
def get_db():
    if not hasattr(g, 'neo4j_db'):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db



@app.route("/")
def home():
    res=graph.query(query).data()  
    data=[x for x in res]
    res2=graph.query(query2).data()  
    data2=[x for x in res2]
    return render_template('index.html',data=data)
@app.route("/q2")
def q():
    res=graph.query(query).data()  
    data=[x for x in res]
    res2=graph.query(query2).data()  
    data2=[x for x in res2]
    res3=graph.query(query3).data()
    data3= [x for x in res3]
    c=len(data3)
    index1=1
    return render_template('index.html',r=res3,data=data,index=index1,count=c)
@app.route("/q3")
def qn():
    res=graph.query(query).data()  
    data=[x for x in res]
    res2=graph.query(query2).data()  
    data2=[x for x in res2]
    res4=graph.query(query4).data()
    cot=len(res4)
    return render_template('index.html',data=data,r2=res4,c=cot)
@app.route("/q1",methods=["GET","POST"])
def qq():
    if request.method == "POST":
        v=request.form['vertical']
        a=request.form['account']
        res=graph.query(query).data()  
        data=[x for x in res]
        res2=graph.query(query2).data()  
        data2=[x for x in res2]
        ak=data2[0]
        print(ak['name'])
        print(request.form['month'])
        print(a)
        query1="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        return v.name as v,oi.name as oi,on.name as on,o.name as o,b.name as b,d.name as d,s.name as s,ot.name as ot,a.name as a,r.name as r,da.name as da"
        res5=graph.query(query1).data()
        queryfreq="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky{name:'TRUE'}),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with ot.name as na,count(ot.name) as co\
        where co>2\
        return count(na) as na"
        resfreq1=graph.query(queryfreq).data()

        queryfreqrisk="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky{name:'TRUE'}),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with ot.name as na,count(ot.name) as co\
        where co>1\
        return count(na) as na"
        resfreqrisk=graph.query(queryfreqrisk).data()


        queryrisky="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky{name:'TRUE'}),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with count(distinct(o.name)) as na\
        return na"
        resrisky=graph.query(queryrisky).data()

        queryriskyopen="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky{name:'TRUE'}),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status{name:'Open'}),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with count(distinct(o.name)) as na\
        return na"
        resriskyopen=graph.query(queryriskyopen).data()

        queryopen="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status{name:'Open'}),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with count(distinct(o.name)) as na\
        return na"
        resopen=graph.query(queryopen).data()

        queryclosed="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status{name:'Closed'}),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status)\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        with count(distinct(o.name)) as na\
        return na"
        resclosed=graph.query(queryclosed).data()
        
        
        c_5=len(res5)
        queryappr="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status{name:'Approved'})\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        return count(distinct(on.name)) as opn"
        resappr=graph.query(queryappr).data()
        queryappr2="match (v:Vertical)-[:VERTICAL]-(oi:Opportunity_ID)-[:OPPORTUNITY_NAME]-(on:Opportunity_Name)-[:OBSERVATION]-(o:Observation)-[:IS_RISKY]-(r:Risky),\
        (b:Billing_Type)-[:BILLING]-(on:Opportunity_Name)-[:DELIVERABLE]-(d:Deliverable_Type),\
        (ot:Observation_Type)-[:OBSERVATION_TYPE]-(o:Observation)-[:STATUS]-(s:Status),\
        (oi:Opportunity_ID)-[:ACCOUNT]-(a:Account),(on:Opportunity_Name)-[:DA_APPROVAL]-(da:DA_Approval_Status{name:'Rejected'})\
        where v.name='"+v+"' and a.name contains'"+a+"'\
        return count(distinct(on.name)) as opn2"
        resappr2=graph.query(queryappr2).data()
        return render_template('index.html',data=data,a=a,v=v,r3=res5,ver=v,freq=resfreq1,c_5=c_5,appr=resappr,appr2=resappr2,freqrisk=resfreqrisk,risky=resrisky,openrisky=resriskyopen,openob=resopen,closedob=resclosed)
    

@app.route("/graph")
def get_graph():
    db = get_db()
    results = db.read_transaction(lambda tx: list(tx.run("MATCH (m:Risky)-[]-(a:Observation)"
                                                        "RETURN m.name as Business_Service, collect(a.name) as Team_Name "
                                                         "LIMIT $limit", {
                                                             "limit": request.args.get("limit",
                                                                                       100)})))
    nodes = []
    rels = []
    i = 0
    for record in results:
        nodes.append({"title": record["Business_Service"], "label": "Business_Service"})
        target = i
        i += 1
        for name in record['Team_Name']:
            TeamName = {"title": name, "label": "TeamName"}
            try:
                source = nodes.index(TeamName)
            except ValueError:
                nodes.append(TeamName)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")

    

   
if __name__=='__main__':
    app.run(debug=True)
