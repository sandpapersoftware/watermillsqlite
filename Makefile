default:
	(cd wmsqlitemodernc && go test -short ./...)
	(cd wmsqlitezombiezen && go test -short ./...)
test:
	(cd wmsqlitemodernc && go test -v -count=5 -timeout=15m ./...)
	(cd wmsqlitezombiezen && go test -v -count=5 -timeout=15m ./...)
test_race:
	(cd wmsqlitemodernc && go test -v -count=5 -timeout=18m -race ./...)
	(cd wmsqlitezombiezen && go test -v -count=5 -timeout=18m -race ./...)
