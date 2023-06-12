﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Testcontainers.PostgreSql;
using Xunit;

namespace Dapper.Tests
{
    public class PostgresProvider : ContainerDatabaseProvider<PostgreSqlBuilder, PostgreSqlContainer>
    {
        public override DbProviderFactory Factory => Npgsql.NpgsqlFactory.Instance;
        protected override string ProviderName => "Postgresql";
    }
    public class PostgresqlTests : TestBase<PostgresProvider>, IClassFixture<PostgresProvider>
    {
        public PostgresqlTests(PostgresProvider provider) : base(provider) { }

        private Npgsql.NpgsqlConnection GetOpenNpgsqlConnection() => (Npgsql.NpgsqlConnection)Provider.GetOpenConnection();

        private class Cat
        {
            public int Id { get; set; }
            public string Breed { get; set; }
            public string Name { get; set; }
        }

        private readonly Cat[] Cats =
        {
            new Cat() { Breed = "Abyssinian", Name="KACTUS"},
            new Cat() { Breed = "Aegean cat", Name="KADAFFI"},
            new Cat() { Breed = "American Bobtail", Name="KANJI"},
            new Cat() { Breed = "Balinese", Name="MACARONI"},
            new Cat() { Breed = "Bombay", Name="MACAULAY"},
            new Cat() { Breed = "Burmese", Name="MACBETH"},
            new Cat() { Breed = "Chartreux", Name="MACGYVER"},
            new Cat() { Breed = "German Rex", Name="MACKENZIE"},
            new Cat() { Breed = "Javanese", Name="MADISON"},
            new Cat() { Breed = "Persian", Name="MAGNA"}
        };

        [Fact]
        public void TestPostgresqlArrayParameters()
        {
            using (var conn = GetOpenNpgsqlConnection())
            {
                IDbTransaction transaction = conn.BeginTransaction();
                conn.Execute("create table tcat ( id serial not null, breed character varying(20) not null, name character varying (20) not null);");
                conn.Execute("insert into tcat(breed, name) values(:Breed, :Name) ", Cats);

                var r = conn.Query<Cat>("select * from tcat where id=any(:catids)", new { catids = new[] { 1, 3, 5 } });
                Assert.Equal(3, r.Count());
                Assert.Equal(1, r.Count(c => c.Id == 1));
                Assert.Equal(1, r.Count(c => c.Id == 3));
                Assert.Equal(1, r.Count(c => c.Id == 5));
                transaction.Rollback();
            }
        }

        [Fact]
        public void TestPostgresqlListParameters()
        {
            using (var conn = GetOpenNpgsqlConnection())
            {
                IDbTransaction transaction = conn.BeginTransaction();
                conn.Execute("create table tcat ( id serial not null, breed character varying(20) not null, name character varying (20) not null);");
                conn.Execute("insert into tcat(breed, name) values(:Breed, :Name) ", new List<Cat>(Cats));

                var r = conn.Query<Cat>("select * from tcat where id=any(:catids)", new { catids = new List<int> { 1, 3, 5 } });
                Assert.Equal(3, r.Count());
                Assert.Equal(1, r.Count(c => c.Id == 1));
                Assert.Equal(1, r.Count(c => c.Id == 3));
                Assert.Equal(1, r.Count(c => c.Id == 5));
                transaction.Rollback();
            }
        }

        private class CharTable
        {
            public int Id { get; set; }
            public char CharColumn { get; set; }
        }

        [Fact]
        public void TestPostgresqlChar()
        {
            using (var conn = GetOpenNpgsqlConnection())
            {
                var transaction = conn.BeginTransaction();
                conn.Execute("create table chartable (id serial not null, charcolumn \"char\" not null);");
                conn.Execute("insert into chartable(charcolumn) values('a');");

                var r = conn.Query<CharTable>("select * from chartable");
                Assert.Single(r);
                Assert.Equal('a', r.Single().CharColumn);
                transaction.Rollback();
            }
        }

        [Fact]
        public void TestPostgresqlSelectArray()
        {
            using (var conn = GetOpenNpgsqlConnection())
            {
                var r = conn.Query<int[]>("select array[1,2,3]").ToList();
                Assert.Single(r);
                Assert.Equal(new[] { 1, 2, 3 }, r.Single());
            }
        }

        [Fact]
        public void TestPostgresqlDateTimeUsage()
        {
            using (var conn = GetOpenNpgsqlConnection())
            {
                DateTime now = DateTime.UtcNow;
                DateTime? nilA = now, nilB = null;
                _ = conn.ExecuteScalar("SELECT @now, @nilA, @nilB::timestamp", new { now, nilA, nilB });
            }
        }
    }
}
