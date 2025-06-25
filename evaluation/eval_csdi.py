from pypots.optim import Adam
from pypots.imputation import CSDI
from pypots.nn.functional import calc_mae, calc_rmse



def run_csdi(n_steps, n_features, dataset_for_training, dataset_for_validating, dataset_for_testing):
    # Initialize and train CSDI model
    csdi = CSDI(
        n_steps=n_steps,
        n_features=n_features,
        n_layers=6,
        n_heads=2,
        n_channels=128,
        d_time_embedding=64,
        d_feature_embedding=32,
        d_diffusion_embedding=128,
        target_strategy="random",
        n_diffusion_steps=50,
        batch_size=32,
        epochs=100,
        patience=3,
        optimizer=Adam(lr=1e-3),
        num_workers=0,
        device=None,
        saving_path=".data/imputation/csdi",
        model_saving_strategy="best",
    )

    csdi.fit(train_set=dataset_for_training, val_set=dataset_for_validating)

    # CSDI has an argument to control the number of sampling times during inference
    csdi_results = csdi.predict(dataset_for_testing, n_sampling_times=10)
    csdi_imputation = csdi_results["imputation"]

    # for error calculation, we need to take the mean value of the multiple samplings for each data sample
    mean_csdi_imputation = csdi_imputation.mean(axis=1)

    mae = calc_mae(
        mean_csdi_imputation,
        dataset_for_testing['X_ori'],
    )
    
    rmse = calc_rmse(
        mean_csdi_imputation,
        dataset_for_testing['X_ori'],
    )
    
    return mae, rmse
