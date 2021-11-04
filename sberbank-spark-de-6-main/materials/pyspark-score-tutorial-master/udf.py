def agg_features(df_transactions):
    import pandas as pd

    agg_features = pd.concat([
        df_transactions.groupby('client_id')['amount_rur'].agg(['sum', 'mean', 'std', 'min', 'max']),
        df_transactions.groupby('client_id')['small_group'].nunique().rename('small_group_nunique'),
    ], axis=1)

    cat_counts_train = pd.concat([
        df_transactions.pivot_table(
            index='client_id', columns='small_group', values='amount_rur', aggfunc='count').fillna(0.0),
        df_transactions.pivot_table(
            index='client_id', columns='small_group', values='amount_rur', aggfunc='mean').fillna(0.0),
        df_transactions.pivot_table(
            index='client_id', columns='small_group', values='amount_rur', aggfunc='std').fillna(0.0),
    ], axis=1, keys=['small_group_count', 'small_group_mean', 'small_group_std'])

    cat_counts_train.columns = ['_'.join(map(str, c)) for c in cat_counts_train.columns.values]

    agg_features_all = pd.concat([agg_features, cat_counts_train], axis=1)

    return agg_features_all


def score_udf(sdf, model, target_class_names, cols_to_save=None):
    schema = ', '.join([f'{col} {dtype}' for col, dtype in sdf.selectExpr(cols_to_save).dtypes])
    schema += ', '
    schema += ', '.join([f'{label} float' for label in target_class_names])

    def _score_udf(it):
        for features_df in it:
            pred = predict(features_df, model, target_class_names, cols_to_save)
            yield pred
    
    scores = sdf.mapInPandas(_score_udf, schema)
    return scores


def predict(features_df, mdl, target_class_names, cols_to_save=None):
    from sklearn.base import is_classifier
    import pandas as pd

    if cols_to_save is not None:
        existing_cols_to_save = list(set(cols_to_save).intersection(features_df.columns))
        res_df = features_df[existing_cols_to_save].copy()
    else:
        res_df = pd.DataFrame()

    if is_classifier(mdl):
        pred = mdl.predict_proba(features_df)

        for i, label in enumerate(target_class_names):
                res_df[label] = pred[:, i]
    else:
        raise AttributeError('unknown model type')

    return res_df


def block_iterator(iterator, size):
    bucket = list()
    for e in iterator:
        bucket.append(e)
        if len(bucket) >= size:
            yield bucket
            bucket = list()
    if bucket:
        yield bucket


def score(sc, sdf, model, cols_to_save=None, target_class_names=None, code_in_pickle=False):
    import json
    import pandas as pd

    cols = sdf.columns

    def block_classify(iterator):
        from sklearn.base import is_classifier, is_regressor

        for features in block_iterator(iterator, 10000):
            features_df = pd.DataFrame(list(features), columns=cols)

            res_df = predict(features_df, model, target_class_names, cols_to_save)

            for e in json.loads(res_df.to_json(orient='records')):
                yield e

    scores = sdf.rdd.mapPartitions(block_classify)
    score_df = scores.toDF()

    return score_df