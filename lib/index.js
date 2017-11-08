'use strict';

// Load modules
const Fs     = require('fs');
const Path   = require('path');
const Hoek   = require('hoek');
const Crypto = require('crypto');
const Mkdirp = require('mkdirp');
const Walk   = require('walk');
const Util   = require('util');
const Boom   = require('boom');

// Declare internals
const internals = {};
const fs = {
    writeFile: Util.promisify(Fs.writeFile),
    readFile: Util.promisify(Fs.readFile),
    unlink: Util.promisify(Fs.unlink),
    stat: Util.promisify(Fs.stat)
};
const mkdirp = Util.promisify(Mkdirp);


internals.testDiskAccess = async (cachePath) => {

    const rando    = Math.floor(Math.random() * (30000 - 500) + 500);
    const filepath = Path.join(cachePath,'testDiskAccess.' + rando + '.txt');
    const body     = 'okey-dokey';

    await fs.writeFile(filepath, body);
    let data = await fs.readFile(filepath, 'utf8')
    Hoek.assert(data === body, `Error in value  "${data}" not equaling "${body}"`);
    await fs.unlink(filepath);
};

internals.Unlink = async function (filepath) {

    try {
        await fs.unlink(filepath);
    } catch (e) {
        if (e.code !== 'ENOENT'){
            throw new Boom(e);
        }
    }
};

exports = module.exports = internals.Connection = class {

    constructor (options) {

        const defaults = { cleanEvery:3600000 };
        this.isConnected = false;
        Hoek.assert(this.constructor === internals.Connection, 'Disk cache client must be instantiated using new');
        const settings = Hoek.applyToDefaults(defaults, options);
        Hoek.assert(settings.cachePath, 'Missing cachePath value');
        Hoek.assert(settings.cleanEvery === parseInt(settings.cleanEvery, 10), 'cleanEvery is not an integer');

        this.settings = Hoek.clone(settings);
    }


    getStoragePathForKey (key) {
        const hash = Crypto.createHash('md5').update(key.id).digest('hex');

        const sub1        = hash.substring(0,2);
        const sub2        = hash.substring(2,4);
        const destination = Path.join(this.settings.cachePath, key.segment, sub1, sub2, hash + '.json');
        // console.log('destination:',destination);
        return destination;
    }



    async start () {

        this.isConnected = false;

        const stats = await fs.stat(this.settings.cachePath);
        if (!stats.isDirectory()) {
            throw new Boom(`cachePath "${this.settings.cachePath}" is not a directory!`);
        }

        await internals.testDiskAccess(this.settings.cachePath);
        this.isConnected = true;
        this.cacheCleanerInit();
    }


    stop () {

        clearTimeout(this.cacheCleanerTimeout);
        this.isConnected = false;
    }

    isReady () {

        return this.isConnected;
    }


    validateSegmentName (name) {

        if (!name) {
            return new Error('Empty string');
        }

        if (name.indexOf('\0') !== -1) {
            return new Error('Includes null character');
        }

        return null;
    }

    async get (key) {

        if (!this.isConnected) {
            throw new Boom('Connection not started');
        }

        const filepath = this.getStoragePathForKey(key);
        return await this.readCacheFile(filepath);
    }

    async readCacheFile (filepath) {

        let data = null;

        try {
            data = await fs.readFile(filepath, 'utf8');
        } catch (e) {
            if (e.code !== 'ENOENT') {
                throw new Boom(e);
            }
            return null;  // File not found = cache miss
        }

        let obj;
        try {
            obj = JSON.parse(data);
        } catch (e){
            // remove the corrupted file to prevent later issues
            return internals.Unlink(filepath);
        }

        // const obj  = JSON.parse(data);
        const now     = new Date().getTime();
        // const stored  = obj.stored;
        const key     = obj.key;
        const ttl     = obj.stored + obj.ttl - now;

        // Cache item has expired
        if (ttl<=0) {
            this.drop(key, () => {}); // clear out the old stuff
            return null;
        }

        const result = {
            key,
            ttl,
            item   : obj.item,
            stored : obj.stored,
        };

        return result;
    }

    async set (key, value, ttl) {

        if (!this.isConnected) {
            throw new Boom('Connection not started');
        }

        const filepath = this.getStoragePathForKey(key);
        const dirs     = Path.dirname(filepath);

        const envelope = {
            key,
            ttl,
            item    : value,
            stored  : Date.now(),
            expires : new Date((new Date()).getTime() + ttl)
        };

        let body = null;
        try {
            body = JSON.stringify(envelope);
        }
        catch (err) {
            throw new Boom(err);
        }

        try {
            await mkdirp(dirs);
            await fs.writeFile(filepath, body);
        } catch (e){
            Hoek.assert(e,`${e}`);
        }

        return null;
    }

    async drop (key) {

        if (!this.isConnected) {
            throw new Boom('Connection not started');
        }

        const filepath = this.getStoragePathForKey(key);
        await internals.Unlink(filepath);
    }

    async cacheCleanerInit (){
        const self = this;

        // early exit if we don't want automated cleanup
        if (self.settings.cleanEvery === 0){
            return;
        }

        const firstrun = Math.floor(Math.random() * (3000 - 200) + 200);
        const runCleaner = function (){
            const walker  = Walk.walk(self.settings.cachePath, { followLinks: false });
            walker.on('file', (root, fileStat, next) => {
                // only examine files matching the cache naming convention, ignore all others
                if (!fileStat.name.match(/^[a-f0-9]{32}\.json$/i)){
                    return next();
                }

                const filepath = Path.resolve(root, fileStat.name);
                self.readCacheFile(filepath, next);

            });

            walker.on('end', () => {

                self.cacheCleanerTimeout = setTimeout(runCleaner,self.settings.cleanEvery);
            });
        };

        self.cacheCleanerTimeout = setTimeout(runCleaner,firstrun);
    }
}
