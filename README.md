# "Flood-It!" User Lifetime Value Prediction
Dataset (BigQuery database) is available at: https://console.cloud.google.com/bigquery?p=firebase-public-project&d=analytics_153293282&t=events_20181003&page=table <br><br>
Data pre-processing using `PySpark.Sql` is done in a `src/processing.ipynb` notebook. <br><br>
SQL queries which were executed on BigQuery to obtain tables `ltv_data`, `event_data`, `session_data` are stored in `src/processing/` directory as `*.txt` files. The only note here is such that these queries contain placeholders for additional data filtering (last `WHERE` clause) which is called by the model web app (see demo video for details). <br><br>
The file `src/tree_models.ipynb` contains the code of final model (**Random Forest** with tuned hyperparameters). In addition, linear models were given a try, see implementation at `src/linear_models.ipynb`. <br><br>
The end product of the work is a service, built around the model, with the following architecture:
<img src="https://sun9-23.userapi.com/impg/yAXTd-CRkCLcCLKPmNjeDe5M9i3bnU1DiAoBOQ/0cfdXmHr998.jpg?size=1968x1104&quality=96&sign=a9aa0a0f8eab73335694b083c0b55690&type=album" width="900" height="500"> <br><br>
Platform demo video: https://drive.google.com/file/d/1LY0GSFp3YV4-35N3chgULBxnN9SX92fW/view?usp=share_link
