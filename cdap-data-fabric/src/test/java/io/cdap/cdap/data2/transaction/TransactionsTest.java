/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.transaction;

import co.cask.cdap.api.annotation.TransactionPolicy;
import org.junit.Assert;
import org.junit.Test;

import static co.cask.cdap.api.annotation.TransactionControl.EXPLICIT;
import static co.cask.cdap.api.annotation.TransactionControl.IMPLICIT;

@SuppressWarnings("unused")
public class TransactionsTest {

  @Test
  public void testTransactionControlAnnotation() {

    // public method defined in class but annotation is in interface
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ClassWithProtected(), "publicF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      EXPLICIT, ClassWithProtected.class, new ClassWithProtected(), "publicF", String.class));

    // public method defined in class, no annotation in class or interface
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ClassWithProtected(), "publicG", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      EXPLICIT, ClassWithProtected.class, new ClassWithProtected(), "publicG", String.class));

    // protected method introduced and annotated in class
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ClassWithProtected(), "protectedF", String.class));

    // protected method introduced but not annotated in class
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ClassWithProtected(), "protectedG", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new ClassWithProtected(), "protectedG", String.class));

    // public method annotated in interface, overridden and annotated in abstract superclass
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "publicF", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "publicF", String.class));

    // public method defined in interface, overridden and annotated in abstract superclass
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "publicG", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      EXPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "publicG", String.class));

    // protected method defined and annotated in super-superclass, overridden but not annotated in abstract superclass
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "protectedF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      EXPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "protectedF", String.class));

    // protected method defined and annotated in super-superclass, overridden and annotated in abstract superclass
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "protectedG", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "protectedG", String.class));

    // abstract method defined and annotated in abstract superclass, overridden but not annotated in class
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "abstractF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      EXPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "abstractF", String.class));

    // abstract method defined in abstract superclass, overridden and annotated in class
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "abstractG", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "abstractG", String.class));

    // public method defined and annotated in abstract super class, overridden but not annotated in class
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "additionalF", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "additionalF", String.class));

    // public method defined but not annotated in class itself
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new OverridingConcreteClass(), "additionalG", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new OverridingConcreteClass(), "additionalG", String.class));

    // ConcreteSubClass should inherot everything from OverridingConcreteClass
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "publicF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "publicG", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "protectedF", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "protectedG", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "abstractF", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "abstractG", String.class));
    Assert.assertEquals(EXPLICIT, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "additionalF", String.class));
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "additionalG", String.class));

    // method not defined in any super class
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "undefinedF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new ConcreteSubClass(), "undefinedF", String.class));

    // method defined in other interface but not annotated
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, ClassWithProtected.class, new ConcreteSubClass(), "otherF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      IMPLICIT, ClassWithProtected.class, new ConcreteSubClass(), "otherF", String.class));

    // test well-behavior in case the bounding type is not a superclass of the class
    Assert.assertEquals(null, Transactions.getTransactionControl(
      null, String.class, new ConcreteSubClass(), "otherF", String.class));
    Assert.assertEquals(IMPLICIT, Transactions.getTransactionControl(
      IMPLICIT, String.class, new ConcreteSubClass(), "otherF", String.class));
  }

  interface Interface {
    @TransactionPolicy(IMPLICIT)
    void publicF(String x);

    void publicG(String x);
  }

  class ClassWithProtected implements Interface {
    @Override
    public void publicF(String x) { }

    @Override
    public void publicG(String x) { }

    @TransactionPolicy(IMPLICIT)
    protected void protectedF(String x) { }

    protected void protectedG(String x) { }
  }

  abstract class OverridingAbstractClass extends ClassWithProtected {
    @Override
    @TransactionPolicy(EXPLICIT)
    public void publicF(String x) { }

    @Override
    @TransactionPolicy(IMPLICIT)
    public void publicG(String x) { }

    @Override
    protected void protectedF(String x) { }

    @Override
    @TransactionPolicy(EXPLICIT)
    protected void protectedG(String x) { }

    @TransactionPolicy(IMPLICIT)
    protected abstract void abstractF(String x);

    protected abstract void abstractG(String x);

    @TransactionPolicy(EXPLICIT)
    public void additionalF(String x) { }
  }

  class OverridingConcreteClass extends OverridingAbstractClass {
    @Override
    protected void abstractF(String x) { }

    @Override
    @TransactionPolicy(EXPLICIT)
    protected void abstractG(String x) { }

    @Override
    public void additionalF(String x) { }

    public void additionalG(String x) { }
  }

  interface OtherInterface {
    void otherF(String x);
  }

  class ConcreteSubClass extends OverridingConcreteClass implements OtherInterface {
    @Override
    public void otherF(String x) { }
  }
}
