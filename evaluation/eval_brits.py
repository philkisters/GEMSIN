from pypots.optim import Adam
from pypots.imputation import BRITS
from pypots.nn.functional import calc_mae, calc_rmse

def run_brits(n_steps, n_features, dataset_for_training, dataset_for_validating, dataset_for_testing):
    brits = BRITS(
        n_steps=n_steps,
        n_features=n_features,
        rnn_hidden_size=20,
        batch_size=32,
        epochs=100,
        patience=3,
        optimizer=Adam(lr=1e-3),
        num_workers=0,
        device=None,
        saving_path=".data/imputation/brits",
        model_saving_strategy="best",
    )

    brits.fit(train_set=dataset_for_training, val_set=dataset_for_validating)

    # brits has an argument to control the number of sampling times during inference
    brits_results = brits.predict(dataset_for_testing)
    brits_imputation = brits_results["imputation"]

    mae = calc_mae(
        brits_imputation,
        dataset_for_testing['X_ori'],
    )
    
    rmse = calc_rmse(
        brits_imputation,
        dataset_for_testing['X_ori'],
    )
    
    return mae, rmse
