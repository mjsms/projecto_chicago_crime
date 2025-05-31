"""Treina modelos e guarda best full_model (pipeline_fe + clf)."""
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
import json, time, os

DATA_PATH = "../dados/chicago_ready.parquet"
PIPELINE_FE_PATH = "../dados/pipeline_fe"
FULL_MODEL_PATH = "../dados/full_model"

spark = (SparkSession.builder
         .appName("Train_Model")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

df = spark.read.parquet(DATA_PATH)
train, test = df.randomSplit([0.8, 0.2], seed=42)

evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

# Models
lr = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label")
rf = RandomForestClassifier(numTrees=50, maxDepth=8, featuresCol="features", labelCol="label")
gbt = GBTClassifier(maxIter=20, maxDepth=8, featuresCol="features", labelCol="label")

grids = {
    "LogReg": ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.1]).build(),
    "RandomForest": ParamGridBuilder().build(),  # simple grid
    "GBT": ParamGridBuilder().build(),
}

models = {"LogReg": lr, "RandomForest": rf, "GBT": gbt}

best_auc = -1
best_name = None
best_model = None

for name, model in models.items():
    print(f"⚙️ Training {name}")
    tvs = TrainValidationSplit(estimator=model,
                               estimatorParamMaps=grids[name],
                               evaluator=evaluator,
                               trainRatio=0.8,
                               parallelism=1)
    start = time.time()
    tvs_model = tvs.fit(train)
    auc = evaluator.evaluate(tvs_model.transform(test))
    print(f"→ {name} AUC={auc:.4f}  time={time.time()-start:.1f}s")
    if auc > best_auc:
        best_auc, best_name, best_model = auc, name, tvs_model.bestModel

print(f"🏆 Best model: {best_name}  AUC={best_auc:.4f}")

# Load pipeline_fe
pipeline_fe = PipelineModel.load(PIPELINE_FE_PATH)
full_model = Pipeline(stages=[pipeline_fe, best_model]).fit(df)
full_model.write().overwrite().save(FULL_MODEL_PATH)
print("✅ Saved full_model to", FULL_MODEL_PATH)

spark.stop()
