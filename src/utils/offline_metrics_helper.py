import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import *


def dcg_at_k(rank_scores, k, method=1):
    """
    Calculate the DCG at K for a list of items
    :param rank_scores: items list
    :param k: ranking threshold
    :param method: scoring method
    :return: DCG at K
    """
    rank_scores = np.asfarray(rank_scores)[:k]
    if rank_scores.size:
        if method == 0:
            return rank_scores[0] + np.sum(rank_scores[1:] / np.log2(np.arange(2, rank_scores.size + 1)))
        elif method == 1:
            return np.sum(rank_scores / np.log2(np.arange(2, rank_scores.size + 2)))
        elif method == 2:
            return np.sum((2 ** rank_scores - 1) / np.log2(np.arange(2, rank_scores.size + 2)))
        else:
            raise ValueError('method must be 0 or 1.')
    return 0.


def ndcg_at_k(rank_scores, k, method=0):
    """
    Calculate the NDCG at K for a list of items
    :param rank_scores: items list
    :param k: ranking threshold
    :param method: scoring method
    :return: NDCG at K
    """
    dcg_max = dcg_at_k(sorted(rank_scores, reverse=True), k, method)
    if not dcg_max:
        return 0.
    return dcg_at_k(rank_scores, k, method) / dcg_max


def get_mAP_mAR(evaluation_df, covered_users):
    """
    Calculate the Mean Average Precision and Mean Average Recall
    :param evaluation_df:
    :param covered_users:
    :return:
    """
    evaluation_df = evaluation_df.withColumn('precision',
                                             F.size(F.array_intersect('listing_pred', 'listing_gt')) / F.size(
                                                 'listing_pred'))
    evaluation_df = evaluation_df.withColumn('recall',
                                             F.size(F.array_intersect('listing_pred', 'listing_gt')) / F.size(
                                                 'listing_gt'))
    metrics_df = evaluation_df.agg(F.mean('precision').alias('mean_precision'),
                                   F.mean('recall').alias('mean_recall'))
    metrics_df = metrics_df.withColumn('user_coverage', F.lit(f'{covered_users}'))
    return metrics_df


def array_func():
    return F.udf(lambda listing_pred, listing_gt: \
                     [1 if listing_id in listing_gt else 0 for listing_id in listing_pred], ArrayType(IntegerType()))


def ndcg_func():
    return F.udf(lambda score, k, m: str(ndcg_at_k(score, int(k), int(m))), StringType())


def generate_metrics(pred_df, gt_df, top_rank, method='2'):
    """
    Generate NDCG, mAP, mAR metrics for a target date
    :param pred_df: Predictions Dataframe
    :param gt_df: Ground Truth Dataframe
    :param top_rank: Rank threshold (K)
    :param method: Scoring Method for NDCG at K
    :return: Dataframe of metrics for a specific top_rank (K)
    """

    pred_df = pred_df.filter(pred_df.rank <= top_rank)
    pred_users = pred_df.select('user_id').distinct()
    gt_users = gt_df.select('user_id').distinct()

    shared_users = pred_users.join(gt_users, on='user_id')
    covered_users = shared_users.count()

    pred_df = pred_df.join(shared_users, on='user_id')
    gt_df = gt_df.join(shared_users, on='user_id')

    pred_grouped = pred_df.groupBy('user_id').agg(F.collect_list('listing_id').alias('listing_pred'),
                                                  F.collect_list('rank').alias('rank'))

    gt_grouped = gt_df.groupBy('user_id').agg(F.collect_list('listing_id').alias('listing_gt'))

    evaluation_df = pred_grouped.join(gt_grouped, on='user_id')

    mAP_mAR_metrics_df = get_mAP_mAR(evaluation_df, covered_users)

    evaluation_df = evaluation_df.withColumn('score', array_func()(F.col('listing_pred'), F.col('listing_gt')))
    evaluation_df = evaluation_df.withColumn('ndcg_str', ndcg_func()(F.col('score'), F.lit(top_rank), F.lit(method)))
    evaluation_df = evaluation_df.withColumn('ndcg_double', evaluation_df.ndcg_str.cast(DoubleType()))

    ndcg_metrics_df = evaluation_df.agg(F.mean('ndcg_double').alias('mean_ndcg'))
    ndcg_metrics_df = ndcg_metrics_df.withColumn('covered_users', F.lit(covered_users))

    return mAP_mAR_metrics_df, ndcg_metrics_df
