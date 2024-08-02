lr = LinearRegression(featuresCol='features', labelCol='label')

lr_model = lr.fit(train_df)