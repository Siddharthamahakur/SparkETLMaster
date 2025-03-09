from processing.ml_model import train_model


def test_train_model():
    X = [1, 2, 3, 4, 5]
    y = [2, 4, 6, 8, 10]

    model = train_model(X, y)
    assert model is not None, "Model should be trained successfully"
