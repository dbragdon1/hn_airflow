
## Working with the Hackernews API

The [Hackernews API](https://github.com/HackerNews/API) is a REST api that allows you to access historic post data through unique post ids. To access any post, you visit the following link:

```
https://hacker-news.firebaseio.com/v0/item/postID.json?print=pretty
```

where `postID` is replaced with the post's unique id. For example, viewing the post with ID `1000` would require visiting:

```
https://hacker-news.firebaseio.com/v0/item/1000.json?print=pretty
```

which results in a json file of attributes corresponding to the post:

```
{
  "by" : "python_kiss",
  "descendants" : 0,
  "id" : 1000,
  "score" : 4,
  "time" : 1172394646,
  "title" : "How Important is the .com TLD?",
  "type" : "story",
  "url" : "http://www.netbusinessblog.com/2007/02/19/how-important-is-the-dot-com/"
}

```
## Scheduling comment ingestion


In order to manage a workflow of inserting new and old posts into the postgres database I used some simple airflow mechanics.

The first step was creating a task that could backfill historic data into the database. This was simple enough, and simply required that only posts older than the oldest post in the database be inserted.


First we start with the helper function:

```python
def get_historic_posts(n: int):
    try:
        conn = psycopg2.connect(
            dbname = 'hackernews',
            user = 'postgres',
            host = 'host.docker.internal',
            password = 'postgres'
        )

        cur = conn.cursor()

        #get oldest post ID from database
        cur.execute("SELECT MIN(info->>'id')::int FROM post_json")
        oldest = cur.fetchone()[0]

        #if table is empty, start from newest post on the API
        if not oldest:
            r = requests.get(newest_id_fmt)
            oldest = r.json()
            r = requests.get(post_fmt.format(oldest))
            obj = r.json()
            cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                        [Json(obj)])
            #decrease n by 1 to account for single post in table
            n -= 1

        #now oldest post in table is either:
          #1. most recent post in API or
          #2. post corresponding to smallest ID in table

        #starting from oldest post in table, count backwards from n
        for i in range(n):
            oldest -= 1
            r = requests.get(post_fmt.format(oldest))
            obj = r.json()
            cur.execute("INSERT INTO post_json (info) VALUES (%s)",
                        [Json(obj)])
        conn.commit()
        conn.close()

    except Error as E:
        print(E)
```

```python
historic_data_dag = DAG(
    dag_id = "collect_historic_data",
    default_args = default_args,
    description = "Get historic post data",
    schedule_interval = None,
    start_date = days_ago(2)
)

get_historic_posts_task = PythonOperator(
                        task_id = 'get_historic_posts',
                        python_callable = get_historic_posts,
                        op_kwargs = {'n': 50},
                        dag = historic_data_dag)
```


1. A task must be created for adding newly created posts
2. A separate task must be created for backfilling the database with historic comments




### Pushing New Comments to database
