"""Small bidirectional LSTM tagger over frozen per-page feature vectors.

Ported from NLP's ``startpage/bilstm_sequence.py``. The whole sequence is available at
inference time (not a streaming decoder), so bidirectional is legitimate. Imports torch.
"""
import torch
import torch.nn as nn

__all__ = ["BiLSTMTagger"]


class BiLSTMTagger(nn.Module):
    def __init__(self, n_features, hidden_size=64):
        super().__init__()
        self.lstm = nn.LSTM(n_features, hidden_size, num_layers=1, bidirectional=True)
        self.head = nn.Linear(2 * hidden_size, 1)

    def forward(self, features):
        """features: (T, n_features) -> (T,) start-page logits."""
        out, _ = self.lstm(features.unsqueeze(1))
        return self.head(out.squeeze(1)).squeeze(-1)

    @torch.no_grad()
    def predict(self, features):
        """(T,) list of bool predictions at the 0.5 probability threshold."""
        return (torch.sigmoid(self.forward(features)) > 0.5).tolist()
