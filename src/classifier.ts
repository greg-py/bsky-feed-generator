import { BayesClassifier } from 'natural'
import { TrainingDataItem } from './algos/digital-agora/training'

export class PostClassifier extends BayesClassifier {
  constructor(trainingData: TrainingDataItem[]) {
    super()
    trainingData.forEach((item) => this.addDocument(item.text, item.label))
    this.train()
  }
}
