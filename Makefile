default:
	(cd wmsqlitemodernc && go test -short -failfast ./...)
	(cd wmsqlitezombiezen && go test -short -failfast ./...)
test:
	(cd wmsqlitemodernc && go test -v -count=5 -failfast -timeout=15m ./...)
	(cd wmsqlitezombiezen && go test -v -count=5 -failfast -timeout=15m ./...)
test_race:
	(cd wmsqlitemodernc && go test -v -count=5 -failfast -timeout=18m -race ./...)
	(cd wmsqlitezombiezen && go test -v -count=5 -failfast -timeout=18m -race ./...)
