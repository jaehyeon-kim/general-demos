class PlaceOrderResponse:
    def __init__(self, isSuccess, errorReason, orderId):
        self.IsSuccess = isSuccess
        self.ErrorReason = errorReason
        self.OrderId = orderId

    # for making JSON serializable, convert to dict
    def asdict(self):
        return {'IsSuccess': self.IsSuccess, 'ErrorReason': self.ErrorReason, 'OrderId': self.OrderId}

