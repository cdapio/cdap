describe("Test TV Switch and Channels", function(){
    var myTv = null;
    
    beforeEach(function() {
        myTv = Tv();
    });
    
    it("turn it on", function(){
        expect(myTv.isOn()).toEqual(false);
        expect(myTv.turnOn()).toEqual(true);
        expect(myTv.isOn()).toEqual(true);
    });
    
    it("turn it on, check channel is 'BBC 1', change channel to 'ITV 1'", function() {
        expect(myTv.isOn()).toEqual(false);
        expect(myTv.turnOn()).toEqual(true);
        expect(myTv.isOn()).toEqual(true);
        
        expect(myTv.channel()).toEqual("BBC 1");
        
        expect(myTv.setChannel("ITV 1")).toEqual(true);
        expect(myTv.channel()).toEqual("ITV 1");
    });
    
    it("change channel to 'RAI 1' while off", function(){
        expect(myTv.setChannel("RAI 1")).toBeFalsy();
    });
});

describe("Test TV Switch", function(){
    var myTv = null;

    beforeEach(function() {
        myTv = Tv();
    });

    it("turn it off", function(){
        expect(myTv.isOn()).toEqual(false);
        expect(myTv.turnOff()).toEqual(false);
        expect(myTv.isOn()).toEqual(false);
        expect(myTv.turnOn()).toEqual(true);
        expect(myTv.isOn()).toEqual(true);
    });
});

describe("Test TV Volume", function(){
    var myTv = null;

    beforeEach(function() {
        myTv = Tv();
    });

    it("try changing the volume while off", function(){
        expect(myTv.setVolume(100)).toEqual(false);
    });

    it("check volume while off", function(){
        expect(myTv.volume()).toEqual(false);
    });

    it("turn the tv on, than change the volume", function(){
        expect(myTv.turnOn()).toEqual(true);
        expect(myTv.volume()).toEqual(50);
        expect(myTv.setVolume(100)).toEqual(true);
        expect(myTv.volume()).toEqual(100);
    });
});