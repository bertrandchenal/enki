package enki

type Backend interface {
	AddBlock(WeakHash, *StrongHash, Block)
	SearchWeak(WeakHash) bool
	GetStrong(*StrongHash) (Block, bool)
	GetSignature(string) (*Signature, bool)
	SetSignature(string, *Signature)
}
