import tensorflow as tf
import tf_dataset

def get_embeddings(applist, promoted_app, vocab_size, embedding_size, id_type):
    with tf.variable_scope("embed_" + id_type):
        embeddings = tf.get_variable(name="embeddings", dtype=tf.float32,
                                   shape=[vocab_size, embedding_size])
        pb_app_emb = tf.nn.embedding_lookup(embeddings, promoted_app)
        return None, pb_app_emb

def get_embeddings_with_applist(applist, promoted_app, vocab_size, embedding_size, id_type):
    with tf.variable_scope("embed_" + id_type):
        embeddings = tf.get_variable(name="embeddings", dtype=tf.float32,
                                   shape=[vocab_size, embedding_size])
        applist_emb = tf.nn.embedding_lookup(embeddings, applist) # [B, N, D]
        pb_app_emb = tf.nn.embedding_lookup(embeddings, promoted_app)
        masks = tf.expand_dims(tf.cast(applist != 'default', tf.float32), axis=-1)
        applist_emb = tf.reduce_sum(tf.multiply(applist_emb, masks), axis=1)
                
        return applist_emb, pb_app_emb

def get_embeddings_with_attention(applist, promoted_app, vocab_size, embedding_size, id_type):
    with tf.variable_scope("embed_" + id_type):
        embeddings = tf.get_variable(name="embeddings", dtype=tf.float32,
            shape=[vocab_size, embedding_size])
        applist_emb = tf.nn.embedding_lookup(embeddings, applist) # [B, N, D]
        max_seq_len = tf.shape(applist_emb)[1]
        pb_app_emb = tf.nn.embedding_lookup(embeddings, promoted_app) # [B, D]
        u_emb = tf.reshape(applist_emb, [-1, embedding_size])
        a_emb = tf.reshape(tf.tile(pb_app_emb, [1, max_seq_len]), [-1, embedding_size])
        
        net = tf.concat([u_emb, u_emb - a_emb, a_emb], axis=1)
        for units in [32, 16]:
            net = tf.layers.dense(net, units=units, activation=tf.nn.relu)
        att_wgt = tf.layers.dense(net, units=1, activation=tf.sigmoid) #[B, N, 1] 
        att_wgt = tf.reshape(att_wgt, shape=[-1, max_seq_len, 1], name="weight") 
        wgt_emb = tf.multiply(applist_emb, att_wgt)
        masks = tf.expand_dims(tf.cast(applist != 'default', tf.float32), axis=-1)
        att_emb = tf.reduce_sum(tf.multiply(wgt_emb, masks), 1, name="weighted_embedding")
        
        return att_emb, pb_app_emb

def ctr_model(features, labels, mode, params):
    common = tf.feature_column.input_layer(features, params['feature_columns'])
    applist_dense = tf.sparse_tensor_to_dense(features["applist"], default_value='default')
    applist = tf.string_to_hash_bucket_fast(applist_dense, 1000000)
    promoted_app = tf.string_to_hash_bucket_fast(features["promoted_app"], 10000) 
    
    applist_embed, pd_app_embed = get_embeddings_with_applist(applist, promoted_app, 1000000, 8, 'applist')
    #applist_embed, pd_app_embed = get_embeddings_with_attention(applist, promoted_app, 1000000, 8, 'applist')

    net = tf.concat([common, applist_embed, pd_app_embed], axis=1)
    for units in params['hidden_units']:
        net = tf.layers.dense(net, units=units, activation=None)
        net = tf.layers.batch_normalization(net, training=(mode == tf.estimator.ModeKeys.TRAIN))
        net = tf.nn.relu(net)
        net = tf.layers.dropout(net, 0.1, training=(mode == tf.estimator.ModeKeys.TRAIN))

    logits = tf.layers.dense(net, 1, activation=None)
    prop = tf.sigmoid(logits)
    if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = {
            'probabilities': prop,
            'logits': logits
        }
        return tf.estimator.EstimatorSpec(mode, predictions=predictions)
    loss = tf.reduce_sum(tf.nn.sigmoid_cross_entropy_with_logits(labels=labels, logits=logits))
    AUC = tf.metrics.auc(labels=labels, predictions=prop)
    label_mean = tf.metrics.mean(labels)
    pred_mean = tf.metrics.mean(prop)
    metrics = {
        'auc': AUC, 
        'label/mean': label_mean, 
        'pred/mean': pred_mean
    }

    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(
            mode, loss=loss, eval_metric_ops=metrics)
    assert mode == tf.estimator.ModeKeys.TRAIN
    optimizer = tf.train.AdagradOptimizer(learning_rate=0.1)
    
    update_ops = tf.get_collection(tf.GraphKeys.UPDATE_OPS)
    with tf.control_dependencies(update_ops):
        train_op = optimizer.minimize(loss, global_step=tf.train.get_global_step())
    
    return tf.estimator.EstimatorSpec(mode, loss=loss, train_op=train_op)

train_epochs = 3
epochs_between_evals = 1
model_dir = './model_with_applist/'
inter_op_parallelism_threads = 10
intra_op_parallelism_threads = 1

def run_loop(name, train_input_fn, eval_input_fn, model_column_fn, early_stop=False):
    deep_columns = model_column_fn()
    model = tf.estimator.Estimator(
        model_fn=ctr_model,
        params={
            'feature_columns': deep_columns,
            'hidden_units': [256, 128, 64, 32],
        },
        config=tf.estimator.RunConfig(model_dir=model_dir, \
                    save_checkpoints_steps=5000)
    )
    for n in range(train_epochs // epochs_between_evals):
        model.train(input_fn=train_input_fn)
        results = model.evaluate(input_fn=eval_input_fn)
        # Display evaluation metrics
        tf.logging.info('Results at epoch %d / %d',
                    (n + 1) * epochs_between_evals, train_epochs)
        tf.logging.info('-' * 60)

        for key in sorted(results):
            tf.logging.info('%s: %s' % (key, results[key]))

batch_size = 256
def run():
    train_file = "./data_generate/train.tfrecord"
    test_file = "./data_generate/test.tfrecord"
    def train_input_fn():
        return tf_dataset.input_fn_tfrecord(
            train_file, epochs_between_evals, True, batch_size)
    def eval_input_fn():
        return tf_dataset.input_fn_tfrecord(test_file, 1, False, batch_size)
    run_loop('cvr_model', train_input_fn=train_input_fn, eval_input_fn=eval_input_fn, 
        model_column_fn=tf_dataset.create_feature_columns)

if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    run()
