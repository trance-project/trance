class StringIdGenerator {
    private static instance: StringIdGenerator;
    private readonly _chars: string;
    private _nextId:number[] = [0];

   private constructor(chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') {
        this._chars = chars;
    }
    static getInstance() {
       if(this.instance){
           return this.instance;
       }
       this.instance = new StringIdGenerator();
    }

    next() {
        const r = [];
        for (const char of this._nextId) {
            r.unshift(this._chars[char]);
        }
        this._increment();
        return r.join('');
    }

    private _increment() {
        for (let i = 0; i < this._nextId.length; i++) {
            const val = ++this._nextId[i];
            if (val >= this._chars.length) {
                this._nextId[i] = 0;
            } else {
                return;
            }
        }
        this._nextId.push(0);
    }

    private *[Symbol.iterator]() {
        while (true) {
            yield this.next();
        }
    }
}

export default StringIdGenerator;