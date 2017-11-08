'use strict';

// Load modules
const Lab    = require('lab');
const Code   = require('code');
const Catbox = require('catbox');
const Disk   = require('..');
const Fs     = require('fs');
const Path   = require('path');
const Tmp    = require('tmp');
const Util   = require('util');

// promisify
const fs = {
    writeFile: Util.promisify(Fs.writeFile),
    readFile: Util.promisify(Fs.readFile),
    unlink: Util.promisify(Fs.unlink),
    chmod: Util.promisify(Fs.chmod),
    stat: Util.promisify(Fs.stat),
    appendFile: Util.promisify(Fs.appendFile),
    exists: Util.promisify(Fs.exists)
};

// Test shortcuts
const lab      = exports.lab = Lab.script();
const describe = lab.describe;
const it       = lab.it;
const expect   = Code.expect;

// setup general options
const tmpcachepath = Tmp.dirSync({ prefix: 'catbox_disk_tmp_', unsafeCleanup: true, mode: '0777' });
const options = { cachePath: tmpcachepath.name, cleanEvery:0 };


describe('Disk', () => {

    lab.after((done) => {

        console.log('removing tmpcachepath:',tmpcachepath.name);
        tmpcachepath.removeCallback();
        return done();
    });


    describe('#constructor', () => {

        it('throws an error if not created with new', (done) => {

            const fn = () => {

                Disk();
            };

            expect(fn).to.throw(Error);
            done();
        });

        it('throws an error with no provided cachePath', (done) => {

            const fn = () => {
                new Catbox.Client(Disk);
            };
            expect(fn).to.throw(Error);
            done();

        });

        it('throws an error with a non-existent cachePath', async () => {

            const client = new Catbox.Client(Disk, { cachePath: '/does/not/exist/yo/ho/ho' });

            try {
                await client.start();
            } catch (e) {
                expect(e).to.be.instanceof(Error)
            }

            expect(client.isReady()).to.equal(false);
        });

        it('throws an error with a non-directory cachePath', async () => {

            const filepath = Path.join(tmpcachepath.name,'diskCacheTestFile.txt');

            await fs.writeFile(filepath,'ok')
            const client = new Catbox.Client(Disk, { cachePath: filepath });

            try {
                await client.start();
            } catch (e) {
                expect(e).to.be.instanceof(Error)
            }

            expect(client.isReady()).to.equal(false);

            await fs.unlink(filepath);
        });

        it('throws an error with a non-integer cleanEvery', (done) => {

            const fn = () => {

                new Catbox.Client(Disk, { cachePath: tmpcachepath.name, cleanEvery: 'notbloodylikely' });
            };
            expect(fn).to.throw(Error);
            done();

        });

        it('errors on a policy with a missing segment name', (done) => {

            const config = {
                expiresIn: 50000
            };

            const fn = () => {

                const client = new Catbox.Client(Disk, options);
                new Catbox.Policy(config, client, '');
            };
            expect(fn).to.throw(Error);
            done();
        });

        it('errors on a policy with a bad segment name', (done) => {

            const config = {
                expiresIn: 50000
            };
            const fn = () => {

                const client = new Catbox.Client(Disk, options);
                new Catbox.Policy(config, client, 'a\0b');
            };
            expect(fn).to.throw(Error);
            done();
        });

    });

    describe('#start', () => {

        it('creates a new connection', async () => {

            const client = new Catbox.Client(Disk, options);
            try {
                await client.start();
            } catch (e){
                console.log(e);
            }
            expect(client.isReady()).to.equal(true);
        });

        it('closes the connection', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            expect(client.isReady()).to.equal(true);
            await client.stop();
            expect(client.isReady()).to.equal(false);

        });

        it('ignored starting a connection twice on same event', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            expect(client.isReady()).to.equal(true);
            await client.start();
            expect(client.isReady()).to.equal(true);
        });

    });

    describe('#get', () => {

        it('returns not found on get when item expired', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.set(key, 'x', 1);

            await setTimeout(async () => {
                const result = await client.get(key);
                expect(result).to.equal(null);
            }, 1000);
        });

        it('returns not found on get when using null key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.get(null);
            } catch (e) {
                expect(e).to.be.instanceof(Error)
            }
        });

        it('errors on get when using invalid key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.get({});
            } catch (e) {
                expect(e).to.be.instanceof(Error)
            }
        });

        it('errors on get when stopped', async () => {

            const client = new Catbox.Client(Disk, options);
            client.stop();
            const key = { id: 'x', segment: 'test' };
            try {
                await client.connection.get(key)
            } catch (e) {
                expect(e).to.be.instanceof(Error)
            }
        });

        it('gets an item after setting it', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: 'test/id?with special%chars&', segment: 'test' };
            await client.set(key, '123', 5000);
            const result = await client.get(key)
            expect(result.item).to.equal('123');
        });

        it('gets a ttl back on a valid key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: 'test/id?with special%chars&', segment: 'test' };
            await client.set(key, {foo:'bar'}, 5000);
            const result = await client.get(key);
            expect(result.item.foo).to.equal('bar');
            expect(result.ttl).to.be.a.number();
        });

        it('throws error on existing unreadable key ', async () => {

            const disk = new Disk(options);
            await disk.start();

            const key = { segment : 'segment', id : 'unreadablekey' };
            const fp  = disk.getStoragePathForKey(key);

            await disk.set(key, 'notok', 2000);

            await fs.chmod(fp,'0222'); // make the file unreadable
            try {
                await disk.get(key);
            } catch (e) {
                expect(e).to.be.instanceof(Error)
                expect(e.code).to.not.equal('ENOENT');
            }

            await fs.unlink(fp);
        });

        it('returns not found on unparseable JSON and removes file', async () => {

            const disk = new Disk(options);
            await disk.start();

            const key = { segment : 'segment', id : 'badjson' };
            const fp  = disk.getStoragePathForKey(key);

            await disk.set(key, 'notok', 2000);

            await fs.appendFile(fp, 'bad data that kills JSON');
            try {
                await disk.get(key);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
                expect(Fs.existsSync(fp)).to.equal(false);
            }
        });

        it('returns not found on missing key', async () => {

            const disk = new Disk(options);
            await disk.start();

            const key = { segment : 'segment', id : 'missingkey' };

            const result = await disk.get(key);
            expect(result).to.not.exist();
        });

    });

    describe('#set', () => {


        it('errors on set when stopped', async () => {

            const client = new Catbox.Client(Disk, options);
            client.stop();
            const key = { id: 'x', segment: 'test' };
            try {
                await client.connection.set(key, 'y', 1);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });


        it('supports empty keys', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: '', segment: 'test' };
            await client.set(key, '123', 5000);

            const result = await client.get(key);
            expect(result.item).to.equal('123');
        });

        it('errors on set when using null key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.set(null, {}, 1000);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });

        it('errors on set when using invalid key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.set({}, {}, 1000);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });

        it('ignores set when using non-positive ttl value', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.set(key, 'y', 0);
        });

        it('fails setting an item with circular references', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            const key = { id: 'circular', segment: 'test' };
            const value = { a: 1 };
            value.b = value;

            try {
                await client.set(key, value, 10);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });

        it('adds an item to the cache object', async () => {

            const key = { segment: 'test', id: 'test' };
            const disk = new Disk(options);

            await disk.start();
            await disk.set(key, 'myvalue', 2000);
            const result = await disk.get(key);
            expect(result.item).to.equal('myvalue');
        });

    });

    describe('#drop', () => {

        it('does not return an expired item', async () => {

            const key = { segment: 'test', id: 'test' };
            const disk = new Disk(options);
            await disk.start();

            await disk.set(key, 'myvalue', 1500)

            const result = await disk.get(key);
            expect(result.item).to.equal('myvalue');
            await setTimeout(async () => {
                const result2 = await disk.get(key);
                expect(result2).to.not.exist();
            }, 1800);
        });

        it('drops an existing item', async () => {
            const client = new Catbox.Client(Disk, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.set(key, '123', 5000);

            const result = await client.get(key);
            expect(result.item).to.equal('123');
            expect(async () => await client.drop(key)).to.not.throw();
        });

        it('drops an item from a missing segment', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            const key = { id: 'x', segment: 'test' };
            expect(async () => await client.drop(key)).to.not.throw();
        });


        it('drops a missing item', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            const key = { id: 'x', segment: 'test' };
            await client.set(key, '123', 2000);
            const result = await client.get(key);
            expect(result.item).to.equal('123');
            expect(async () => await client.drop({ id: 'y', segment: 'test' })).to.not.throw();
        });


        it('errors on an undroppable file', async () => {

            const disk = new Disk(options);
            await disk.start();

            const key = { segment : 'segment', id : 'undropablekey' };
            const fp  = disk.getStoragePathForKey(key);

            await disk.set(key, 'notok', 2000)

            const dir = Path.dirname(fp);
            Fs.chmodSync(dir,'0555'); // make the file unreadable
            try {
                await disk.drop(key)
            } catch (e) {
                expect(e).to.be.instanceof(Error);
                expect(e.code).to.not.equal('ENOENT');
            }

            Fs.chmodSync(dir,'0777');
            Fs.unlinkSync(fp);
        });

        it('errors on drop when using invalid key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.drop({});
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });


        it('errors on drop when using null key', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.start();
            try {
                await client.drop(null);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });


        it('errors on drop when stopped', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.stop();
            const key = { id: 'x', segment: 'test' };
            try {
                await client.connection.drop(key);
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });


        it('errors when cache item dropped while stopped', async () => {

            const client = new Catbox.Client(Disk, options);
            await client.stop();
            try {
                await client.drop('a')
            } catch (e) {
                expect(e).to.be.instanceof(Error);
            }
        });
    });

    describe('#validateSegmentName', () => {

        it('errors when the name is empty', (done) => {

            const disk = new Disk(options);
            const result = disk.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });


        it('errors when the name has a null character', (done) => {

            const disk = new Disk(options);
            const result = disk.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });


        it('returns null when there are no errors', (done) => {

            const disk = new Disk(options);
            const result = disk.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });

    describe('#cacheCleanerInit', () => {

        it('ignores filenames not matching the cache naming scheme', {timeout:8000}, async () => {

            const disk = new Disk({ cachePath: tmpcachepath.name });

            const keepfp = Path.join(tmpcachepath.name,'test.keep');
            await fs.writeFile(keepfp,'ok','utf8');

            const key = { segment:'segment', id:'removablekey' };
            const removefp  = disk.getStoragePathForKey(key).split('/').slice(-1)[0];

            await fs.writeFile(Path.join(tmpcachepath.name,removefp),'{}','utf8');

            await disk.cacheCleanerInit();
            await setTimeout(async ()=>{
                expect(await fs.exists(keepfp)).to.be.equal(true);
                expect(await fs.exists(removefp)).to.be.equal(false);
            }, 4000);

        });

    });


});
