from pypots.optim import Adam
from pypots.imputation import SAITS
from pypots.nn.functional import calc_mae, calc_rmse

def run_saits(n_steps, n_features,dataset_for_training, dataset_for_validating, dataset_for_testing):
    saits = SAITS(
        n_steps=n_steps,
        n_features=n_features,
        n_layers=2,
        d_model=256,
        d_ffn=128,
        n_heads=4,
        d_k=64,
        d_v=64,
        dropout=0.1,
        attn_dropout=0.1,
        diagonal_attention_mask=True,  # otherwise the original self-attention mechanism will be applied
        ORT_weight=1,  # you can adjust the weight values of arguments ORT_weight
        # and MIT_weight to make the SAITS model focus more on one task. Usually you can just leave them to the default values, i.e. 1.
        MIT_weight=1,
        batch_size=32,
        # here we set epochs=10 for a quick demo, you can set it to 100 or more for better performance
        epochs=100,
        # here we set patience=3 to early stop the training if the evaluting loss doesn't decrease for 3 epoches.
        # You can leave it to defualt as None to disable early stopping.
        patience=3,
        # give the optimizer. Different from torch.optim.Optimizer, you don't have to specify model's parameters when
        # initializing pypots.optim.Optimizer. You can also leave it to default. It will initilize an Adam optimizer with lr=0.001.
        optimizer=Adam(lr=1e-3),
        # this num_workers argument is for torch.utils.data.Dataloader. It's the number of subprocesses to use for data loading.
        # Leaving it to default as 0 means data loading will be in the main process, i.e. there won't be subprocesses.
        # You can increase it to >1 if you think your dataloading is a bottleneck to your model training speed
        num_workers=0,
        # just leave it to default as None, PyPOTS will automatically assign the best device for you.
        # Set it as 'cpu' if you don't have CUDA devices. You can also set it to 'cuda:0' or 'cuda:1' if you have multiple CUDA devices, even parallelly on ['cuda:0', 'cuda:1']
        device=None,  
        # set the path for saving tensorboard and trained model files 
        saving_path=".data/imputation/saits",
        # only save the best model after training finished.
        # You can also set it as "better" to save models performing better ever during training.
        model_saving_strategy="best",
    )

    saits.fit(train_set=dataset_for_training, val_set=dataset_for_validating)

    # saits has an argument to control the number of sampling times during inference
    saits_results = saits.predict(dataset_for_testing)
    saits_imputation = saits_results["imputation"]

    mae = calc_mae(
        saits_imputation,
        dataset_for_testing['X_ori'],
    )
    
    rmse = calc_rmse(
        saits_imputation,
        dataset_for_testing['X_ori'],
    )
    
    return mae, rmse
