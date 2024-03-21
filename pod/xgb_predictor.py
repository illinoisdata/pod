import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
from xgboost import XGBClassifier
import os
from sklearn.model_selection import train_test_split
import pickle



class XGBPredictor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
    
    def train(self):
        feature_paths = []
        for f in os.listdir(self.data_dir):
            feature_paths.append(os.path.join(self.data_dir, f))
        df = pd.concat([pd.read_csv(feature_path) for feature_path in feature_paths], ignore_index=True)
        X_df = df[[
            # "oid",
            "pod_size",
            "pod_max_change_prob",
            "oid_count",
            "oid_change_count",
            "oid_change_prob",
            # "obj_type",
            "obj_len",
        ]]
        # X_df.loc[:,"obj_type"] = X_df["obj_type"].astype("category")
        ys = df["has_changed"].astype("int").values

        X_train, X_test, y_train, y_test = train_test_split(X_df, ys, test_size=.2)
        pos_fraction = y_train.sum() / len(y_train)
        neg_fraction = 1 - pos_fraction
        neg_pos_ratio = neg_fraction / pos_fraction
        est = XGBClassifier(enable_categorical=True, n_estimators=3, scale_pos_weight=neg_pos_ratio)
        est.fit(X_train, y_train)
        y_test_pred = est.predict(X_test)
        acc = (y_test == y_test_pred).mean()
        self.est = est
    

    def predict(self, data):
        probs = self.est.predict_proba(data)
        return probs

    def save(self, save_path):
        with open(save_path, "wb") as model_file:
            pickle.dump(self.est, model_file)
    
    def load(self, load_path):
        with open(load_path, "rb") as model_file:
            model = pickle.load(model_file)
        return model