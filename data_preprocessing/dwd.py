from benchpots.utils import create_missingness, sliding_window
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def preprocess_dwd(data):
  """
  Prepares the DWD dataset of a sensor for use with PyPots.
  This function splits the given sensor measurements (as a DataFrame) into training, validation, and test sets,
  normalizes the numerical columns (excluding the 'timestamp'), creates sliding windows, and introduces artificial missing values.
  The result is a dictionary containing the processed datasets in PyPots format, each with original and missingness variants.
  Args:
    data (pd.DataFrame): DataFrame containing time series measurements of a sensor. Columns represent different measurements,
              and a 'timestamp' column is excluded from normalization.
  Returns:
    dict: Dictionary with the following keys:
      - "n_steps": Length of the sliding windows (int)
      - "n_features": Number of used features (int)
      - "scaler": The scaler used (StandardScaler)
      - "train_X": Training data with artificially missing values (np.ndarray)
      - "train_X_ori": Original training data without missingness (np.ndarray)
      - "val_X": Validation data with artificially missing values (np.ndarray)
      - "va_X_ori": Original validation data without missingness (np.ndarray)
      - "test_X": Test data with artificially missing values (np.ndarray)
      - "test_X_ori": Original test data without missingness (np.ndarray)
  """
  
  # 80% Training, 20% Test
  train_val, test = train_test_split(data, test_size=0.2, shuffle=False)

  # 75% Training, 25% Validation
  train, val = train_test_split(train_val, test_size=0.25, shuffle=False)

  # features contain all rows but (not timestamps)
  features = [col for col in train.columns if col != 'timestamp']
  
  scaler = StandardScaler()
  train_X = train.copy()
  val_X = val.copy()
  test_X = test.copy()

  train_X = scaler.fit_transform(train[features])
  val_X = scaler.transform(val[features])
  test_X = scaler.transform(test[features])

  n_steps = 150
  missing_seq_len = 10
  train_X_ori = sliding_window(train_X, n_steps)
  val_X_ori = sliding_window(val_X, n_steps)
  test_X_ori = sliding_window(test_X, n_steps)

  train_X = create_missingness(train_X_ori, 0.2, "subseq", seq_len=missing_seq_len)
  val_X = create_missingness(val_X_ori, 0.2, "subseq", seq_len=missing_seq_len)
  test_X = create_missingness(test_X_ori, 0.2, "subseq", seq_len=missing_seq_len)


  processed_dataset = {
      # general info
      "n_steps": n_steps,
      "n_features": len(features),
      "scaler": scaler,
      # train set
      "train_X": train_X,
      "train_X_ori": train_X_ori,
      # val set
      "val_X": val_X,
      "val_X_ori": val_X_ori,
      # test set
      "test_X": test_X,
      "test_X_ori": test_X_ori
  }
  
  return processed_dataset